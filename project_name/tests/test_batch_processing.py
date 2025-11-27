import unittest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
import datetime
import os
import tempfile
import shutil

# Import the module to test
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.batch_processing import clean_data, create_or_update_ordersummary, create_customeraggregatespend

class TestBatchProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = (SparkSession.builder
                    .appName("TestCustomerOrderProcessing")
                    .master("local[*]")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .getOrCreate())
        
        # Create temporary directory for test data
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog = "test_catalog"
        cls.schema = "test_schema"
        
        # Create test catalog and schema
        cls.spark.sql(f"CREATE DATABASE IF NOT EXISTS {cls.catalog}.{cls.schema}")
        
        # Create sample data
        cls.create_sample_data()
    
    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()
        
        # Clean up temporary directory
        shutil.rmtree(cls.temp_dir)
    
    @classmethod
    def create_sample_data(cls):
        # Create customer sample data
        customer_data = [
            (1, "John Doe", "john@example.com", "North"),
            (2, "Jane Smith", "jane@example.com", "South"),
            (3, "Bob Johnson", "bob@example.com", "East"),
            (4, "Alice Brown", "alice@example.com", "West"),
            (5, "Charlie Davis", None, "North"),  # Null value
            (5, "Charlie Davis", "charlie@example.com", "North"),  # Duplicate
            (None, "Invalid", "invalid@example.com", "Unknown")  # Null key
        ]
        
        customer_schema = StructType([
            StructField("CustId", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, schema=customer_schema)
        
        # Create order sample data
        order_data = [
            (101, "Laptop", 1200.0, 1, "2023-01-15", 1),
            (102, "Phone", 800.0, 2, "2023-01-20", 2),
            (103, "Tablet", 500.0, 1, "2023-02-05", 3),
            (104, "Monitor", 300.0, 2, "2023-02-10", 4),
            (105, "Keyboard", 50.0, 3, "2023-03-01", 1),
            (106, "Mouse", 25.0, 2, "2023-03-05", 2),
            (107, "Headphones", None, 1, "2023-03-10", 3),  # Null price
            (107, "Headphones", 100.0, 1, "2023-03-10", 3),  # Duplicate
            (None, "Invalid", 10.0, 1, "2023-04-01", 5)  # Null key
        ]
        
        order_schema = StructType([
            StructField("OrderId", IntegerType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("CustId", IntegerType(), True)
        ])
        
        cls.order_df = cls.spark.createDataFrame(order_data, schema=order_schema)
    
    def test_clean_data(self):
        # Test data cleaning function
        cleaned_customer_df, cleaned_order_df = clean_data(self.customer_df, self.order_df)
        
        # Check customer data cleaning
        self.assertEqual(cleaned_customer_df.count(), 4)  # Should have 4 valid records
        self.assertEqual(cleaned_customer_df.filter(F.col("CustId") == 5).count(), 1)  # Duplicates removed
        
        # Check order data cleaning
        self.assertEqual(cleaned_order_df.count(), 6)  # Should have 6 valid records
        self.assertEqual(cleaned_order_df.filter(F.col("OrderId") == 107).count(), 1)  # Duplicates removed
        
        # Check TotalAmount calculation
        order_with_laptop = cleaned_order_df.filter(F.col("ItemName") == "Laptop").collect()[0]
        self.assertEqual(order_with_laptop["TotalAmount"], 1200.0 * 1)

    def test_create_or_update_ordersummary(self):
        # Clean the data first
        cleaned_customer_df, cleaned_order_df = clean_data(self.customer_df, self.order_df)
        
        # Create ordersummary table
        create_or_update_ordersummary(self.spark, cleaned_customer_df, cleaned_order_df, self.catalog, self.schema)
        
        # Check if table was created
        ordersummary_df = self.spark.table(f"{self.catalog}.{self.schema}.ordersummary")
        self.assertTrue(ordersummary_df.count() > 0)
        
        # Check if SCD Type 2 columns exist
        self.assertTrue("IsActive" in ordersummary_df.columns)
        self.assertTrue("StartDate" in ordersummary_df.columns)
        self.assertTrue("EndDate" in ordersummary_df.columns)
        
        # All records should be active in initial load
        self.assertEqual(ordersummary_df.filter(F.col("IsActive") == True).count(), ordersummary_df.count())
        
        # Test update scenario - modify a customer
        updated_customer_data = [
            (1, "John Doe Updated", "john_updated@example.com", "North-East")  # Updated record
        ]
        
        updated_customer_schema = StructType([
            StructField("CustId", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        updated_customer_df = self.spark.createDataFrame(updated_customer_data, schema=updated_customer_schema)
        
        # Update ordersummary with new customer data
        create_or_update_ordersummary(self.spark, updated_customer_df, cleaned_order_df, self.catalog, self.schema)
        
        # Check updated ordersummary table
        updated_ordersummary_df = self.spark.table(f"{self.catalog}.{self.schema}.ordersummary")
        
        # Should have inactive records now
        self.assertTrue(updated_ordersummary_df.filter(F.col("IsActive") == False).count() > 0)
        
        # Check if customer with ID 1 has both active and inactive records
        cust1_records = updated_ordersummary_df.filter(F.col("CustId") == 1)
        self.assertTrue(cust1_records.filter(F.col("IsActive") == True).count() > 0)
        self.assertTrue(cust1_records.filter(F.col("IsActive") == False).count() > 0)

    def test_create_customeraggregatespend(self):
        # First ensure we have the ordersummary table
        cleaned_customer_df, cleaned_order_df = clean_data(self.customer_df, self.order_df)
        create_or_update_ordersummary(self.spark, cleaned_customer_df, cleaned_order_df, self.catalog, self.schema)
        
        # Create the aggregate table
        create_customeraggregatespend(self.spark, self.catalog, self.schema)
        
        # Check if table was created
        agg_df = self.spark.table(f"{self.catalog}.{self.schema}.customeraggregatespend")
        self.assertTrue(agg_df.count() > 0)
        
        # Check schema
        self.assertTrue("Name" in agg_df.columns)
        self.assertTrue("TotalAmount" in agg_df.columns)
        self.assertTrue("Date" in agg_df.columns)
        
        # Verify aggregation - John Doe should have two orders (Laptop and Keyboard)
        john_agg = agg_df.filter(F.col("Name") == "John Doe").collect()
        self.assertEqual(len(john_agg), 2)  # Should have two dates

if __name__ == "__main__":
    unittest.main()