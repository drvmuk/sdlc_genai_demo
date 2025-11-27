import unittest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import os
import tempfile
import shutil

# Note: Testing DLT pipelines directly is challenging as they're designed to run in the Databricks environment.
# This test file provides a framework for unit testing the individual transformations that would be applied in the DLT pipeline.

class TestDeltaLiveTables(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = (SparkSession.builder
                    .appName("TestDeltaLiveTables")
                    .master("local[*]")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .getOrCreate())
        
        # Create temporary directory for test data
        cls.temp_dir = tempfile.mkdtemp()
        
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
    
    def test_customer_silver_transformation(self):
        """Test the transformation that would be applied in customer_silver DLT table."""
        # Apply the transformation logic from the DLT pipeline
        silver_customer_df = (
            self.customer_df
            .filter(
                (F.col("CustId").isNotNull()) &
                (F.col("Name").isNotNull()) &
                (F.col("EmailId").isNotNull()) &
                (F.col("Region").isNotNull())
            )
            .dropDuplicates(["CustId"])
        )
        
        # Verify transformation
        self.assertEqual(silver_customer_df.count(), 4)  # Should have 4 valid records
        self.assertEqual(silver_customer_df.filter(F.col("CustId") == 5).count(), 0)  # Null email record removed
        
    def test_order_silver_transformation(self):
        """Test the transformation that would be applied in order_silver DLT table."""
        # Apply the transformation logic from the DLT pipeline
        silver_order_df = (
            self.order_df
            .filter(
                (F.col("OrderId").isNotNull()) &
                (F.col("ItemName").isNotNull()) &
                (F.col("PricePerUnit").isNotNull()) &
                (F.col("Qty").isNotNull()) &
                (F.col("Date").isNotNull()) &
                (F.col("CustId").isNotNull())
            )
            .dropDuplicates(["OrderId"])
            .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        )
        
        # Verify transformation
        self.assertEqual(silver_order_df.count(), 6)  # Should have 6 valid records
        self.assertEqual(silver_order_df.filter(F.col("OrderId") == 107).count(), 0)  # Null price record removed
        
        # Check TotalAmount calculation
        laptop_order = silver_order_df.filter(F.col("ItemName") == "Laptop").collect()[0]
        self.assertEqual(laptop_order["TotalAmount"], 1200.0)  # 1200 * 1
        
        keyboard_order = silver_order_df.filter(F.col("ItemName") == "Keyboard").collect()[0]
        self.assertEqual(keyboard_order["TotalAmount"], 150.0)  # 50 * 3
    
    def test_ordersummary_join(self):
        """Test the join logic that would be used in the ordersummary DLT table."""
        # First apply the silver transformations
        silver_customer_df = (
            self.customer_df
            .filter(
                (F.col("CustId").isNotNull()) &
                (F.col("Name").isNotNull()) &
                (F.col("EmailId").isNotNull()) &
                (F.col("Region").isNotNull())
            )
            .dropDuplicates(["CustId"])
        )
        
        silver_order_df = (
            self.order_df
            .filter(
                (F.col("OrderId").isNotNull()) &
                (F.col("ItemName").isNotNull()) &
                (F.col("PricePerUnit").isNotNull()) &
                (F.col("Qty").isNotNull()) &
                (F.col("Date