import unittest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType
import datetime
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from data_processing import (
    clean_data,
    process_order_data,
    read_source_data
)

class TestDataProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestDataProcessing") \
            .master("local[*]") \
            .getOrCreate()
        
        # Define test schemas
        cls.customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        # Create test data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C004", None, "alice@example.com", "West"),
            ("C001", "John Doe", "john@example.com", "North")  # Duplicate
        ]
        
        order_data = [
            ("O001", "Laptop", 1000.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Phone", 500.0, 1, datetime.date(2023, 1, 20), "C002"),
            ("O003", "Tablet", 300.0, 3, datetime.date(2023, 1, 25), "C003"),
            ("O004", "Monitor", 200.0, None, datetime.date(2023, 1, 30), "C001"),
            ("O001", "Laptop", 1000.0, 2, datetime.date(2023, 1, 15), "C001")  # Duplicate
        ]
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, cls.customer_schema)
        cls.order_df = cls.spark.createDataFrame(order_data, cls.order_schema)
        
        # Create temporary files for testing
        cls.customer_path = "/tmp/test_customer_data"
        cls.order_path = "/tmp/test_order_data"
        
        cls.customer_df.write.mode("overwrite").option("header", "true").csv(cls.customer_path)
        cls.order_df.write.mode("overwrite").option("header", "true").csv(cls.order_path)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_read_source_data(self):
        customer_df, order_df = read_source_data(self.spark, self.customer_path, self.order_path)
        
        self.assertEqual(customer_df.count(), 5)
        self.assertEqual(order_df.count(), 5)
        self.assertTrue("CustId" in customer_df.columns)
        self.assertTrue("OrderId" in order_df.columns)
    
    def test_clean_data(self):
        clean_customer_df = clean_data(self.customer_df)
        clean_order_df = clean_data(self.order_df)
        
        # Check nulls are removed
        self.assertEqual(clean_customer_df.count(), 3)
        self.assertEqual(clean_order_df.count(), 3)
        
        # Check duplicates are removed
        self.assertEqual(clean_customer_df.filter(F.col("CustId") == "C001").count(), 1)
        self.assertEqual(clean_order_df.filter(F.col("OrderId") == "O001").count(), 1)
    
    def test_process_order_data(self):
        processed_order_df = process_order_data(self.order_df)
        
        # Check TotalAmount column is added
        self.assertTrue("TotalAmount" in processed_order_df.columns)
        
        # Check calculation is correct
        order_with_total = processed_order_df.filter(F.col("OrderId") == "O001").first()
        self.assertEqual(order_with_total["TotalAmount"], 2000.0)  # 1000 * 2

if __name__ == "__main__":
    unittest.main()