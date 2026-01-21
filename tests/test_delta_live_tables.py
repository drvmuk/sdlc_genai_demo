import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
import datetime
import tempfile
import shutil
import os

class TestDeltaLiveTables(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a local Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDeltaLiveTables") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create temporary directories for test data
        cls.temp_dir = tempfile.mkdtemp()
        cls.customer_data_path = os.path.join(cls.temp_dir, "customer_data")
        cls.order_data_path = os.path.join(cls.temp_dir, "order_data")
        
        # Create sample data
        # Customer data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            (None, "Invalid Customer", "invalid@example.com", "West"),  # Null CustId
            ("C001", "John Doe", "john@example.com", "North")  # Duplicate
        ]
        
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, schema=customer_schema)
        cls.customer_df.write.csv(cls.customer_data_path, header=True)
        
        # Order data
        order_data = [
            ("O001", "Laptop", 1000.0, 1, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Phone", 500.0, 2, datetime.date(2023, 1, 20), "C002"),
            ("O003", "Tablet", 300.0, 3, datetime.date(2023, 1, 25), "C003"),
            ("O004", "Monitor", 200.0, 2, datetime.date(2023, 1, 15), "C001"),
            (None, "Invalid Order", 100.0, 1, datetime.date(2023, 1, 30), "C002"),  # Null OrderId
            ("O001", "Laptop", 1000.0, 1, datetime.date(2023, 1, 15), "C001")  # Duplicate
        ]
        
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        cls.order_df = cls.spark.createDataFrame(order_data, schema=order_schema)
        cls.order_df.write.csv(cls.order_data_path, header=True)
    
    @classmethod
    def tearDownClass(cls):
        # Clean up temporary directories
        shutil.rmtree(cls.temp_dir)
        cls.spark.stop()
    
    def test_customer_data_cleaning(self):
        # Test customer data cleaning (removing nulls and duplicates)
        df = self.spark.read.csv(self.customer_data_path, header=True, inferSchema=True)
        cleaned_df = df.filter(col("CustId").isNotNull()).dropDuplicates()
        
        # Should have 3 rows after cleaning
        self.assertEqual(cleaned_df.count(), 3)
        
        # Check if all required columns exist
        for column in ["CustId", "Name", "EmailId", "Region"]:
            self.assertTrue(column in cleaned_df.columns)
    
    def test_order_data_cleaning_and_total_amount(self):
        # Test order data cleaning and TotalAmount calculation
        df = self.spark.read.csv(self.order_data_path, header=True, inferSchema=True)
        cleaned_df = df.filter(col("OrderId").isNotNull()).dropDuplicates()
        
        # Add TotalAmount column
        with_total = cleaned_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        
        # Should have 4 rows after cleaning
        self.assertEqual(with_total.count(), 4)
        
        # Check TotalAmount calculations
        total_amounts = {row["OrderId"]: row["TotalAmount"] for row in with_total.collect()}
        self.assertEqual(total_amounts["O001"], 1000.0)  # 1000 * 1
        self.assertEqual(total_amounts["O002"], 1000.0)  # 500 * 2
        self.assertEqual(total_amounts["O003"], 900.0)   # 300 * 3
        self.assertEqual(total_amounts["O004"], 400.0)   # 200 * 2
    
    def test_join_customer_and_order(self):
        # Test joining customer and order data
        customer_df = self.spark.read.csv(self.customer_data_path, header=True, inferSchema=True)
        order_df = self.spark.read.csv(self.order_data_path, header=True, inferSchema=True)
        
        # Clean data
        cleaned_customer = customer_df.filter(col("CustId").isNotNull()).dropDuplicates()
        cleaned_order = order_df.filter(col("OrderId").isNotNull()).dropDuplicates()
        
        # Add TotalAmount
        order_with_total = cleaned_order.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        
        # Join data
        joined_df = cleaned_customer.join(order_with_total, on="CustId", how="inner")
        
        # Should have 4 rows after joining
        self.assertEqual(joined_df.count(), 4)
        
        # Check if all required columns exist in joined data
        required_columns = ["CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", 
                           "PricePerUnit", "Qty", "Date", "TotalAmount"]
        for column in required_columns:
            self.assertTrue(column in joined_df.columns)
    
    def test_customer_aggregate_spend(self):
        # Test customer aggregate spend calculation
        customer_df = self.spark.read.csv(self.customer_data_path, header=True, inferSchema=True)
        order_df = self.spark.read.csv(self.order_data_path, header=True, inferSchema=True)
        
        # Clean data
        cleaned_customer = customer_df.filter(col("CustId").isNotNull()).dropDuplicates()
        cleaned_order = order_df.filter(col("OrderId").isNotNull()).dropDuplicates()
        
        # Add TotalAmount
        order_with_total = cleaned_order.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        
        # Join data
        joined_df = cleaned_customer.join(order_with_total, on="CustId", how="inner")
        
        # Add SCD Type 2 columns for testing
        scd_df = joined_df.withColumn("IsActive", lit(True)) \
                         .withColumn("StartDate", current_timestamp()) \
                         .withColumn("EndDate", lit(None).cast(TimestampType()))
        
        # Aggregate spend by Name and Date
        agg_df = scd_df.groupBy("Name", "Date").sum("TotalAmount").withColumnRenamed("sum(TotalAmount)", "TotalAmount")
        
        # Check aggregation results
        self.assertEqual(agg_df.count(), 3)  # 3 unique Name-Date combinations
        
        # Convert to dictionary for easy checking
        agg_results = {(row["Name"], row["Date"].isoformat()): row["TotalAmount"] for row in agg_df.collect()}
        
        # John has two orders on 2023-01-15 (1000 + 400 = 1400)
        self.assertEqual(agg_results[("John Doe", "2023-01-15")], 1400.0)
        
        # Jane has one order on 2023-01-20 (1000)
        self.assertEqual(agg_results[("Jane Smith", "2023-01-20")], 1000.0)
        
        # Bob has one order on 2023-01-25 (900)
        self.assertEqual(agg_results[("Bob Johnson", "2023-01-25")], 900.0)

if __name__ == "__main__":
    unittest.main()