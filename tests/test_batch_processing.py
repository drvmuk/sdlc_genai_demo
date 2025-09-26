import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import sys
import os

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.batch_processing import (
    clean_customer_data,
    clean_order_data,
    create_or_update_scd_type2_table
)

class TestBatchProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestCustomerOrderProcessing") \
            .master("local[*]") \
            .getOrCreate()
        
        # Define schemas
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
        
        # Create sample data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C004", None, "alice@example.com", "West"),
            ("C005", "Tom Brown", None, "North"),
            ("C006", "Sarah Lee", "sarah@example.com", None),
            (None, "Mike Wilson", "mike@example.com", "South"),
            ("C001", "John Doe", "john@example.com", "North"),  # Duplicate
            ("C008", "Null", "chris@example.com", "West")
        ]
        
        order_data = [
            ("O001", "Laptop", 1000.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Phone", 500.0, 1, datetime.date(2023, 2, 20), "C002"),
            ("O003", "Tablet", 300.0, 3, datetime.date(2023, 3, 10), "C003"),
            ("O004", "Headphones", 50.0, 4, datetime.date(2023, 4, 5), "C001"),
            ("O005", None, 200.0, 1, datetime.date(2023, 5, 12), "C002"),
            ("O006", "Keyboard", None, 2, datetime.date(2023, 6, 8), "C003"),
            ("O007", "Mouse", 25.0, None, datetime.date(2023, 7, 19), "C001"),
            ("O008", "Monitor", 150.0, 1, None, "C002"),
            ("O009", "Printer", 120.0, 1, datetime.date(2023, 9, 3), None),
            ("O001", "Laptop", 1000.0, 2, datetime.date(2023, 1, 15), "C001"),  # Duplicate
            ("O011", "Null", 75.0, 2, datetime.date(2023, 11, 7), "C003")
        ]
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, cls.customer_schema)
        cls.order_df = cls.spark.createDataFrame(order_data, cls.order_schema)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_clean_customer_data(self):
        cleaned_df = clean_customer_data(self.customer_df)
        
        # Check row count (should remove nulls and duplicates)
        self.assertEqual(cleaned_df.count(), 3)
        
        # Check that all required fields are not null
        self.assertEqual(cleaned_df.filter(F.col("CustId").isNull()).count(), 0)
        self.assertEqual(cleaned_df.filter(F.col("Name").isNull()).count(), 0)
        self.assertEqual(cleaned_df.filter(F.col("EmailId").isNull()).count(), 0)
        self.assertEqual(cleaned_df.filter(F.col("Region").isNull()).count(), 0)
        
        # Check that "Null" string values are removed
        self.assertEqual(cleaned_df.filter(F.col("Name") == "Null").count(), 0)
    
    def test_clean_order_data(self):
        cleaned_df = clean_order_data(self.order_df)
        
        # Check row count (should remove nulls and duplicates)
        self.assertEqual(cleaned_df.count(), 3)
        
        # Check TotalAmount calculation
        order1 = cleaned_df.filter(F.col("OrderId") == "O001").first()
        self.assertEqual(order1["TotalAmount"], 2000.0)  # 1000 * 2
        
        order2 = cleaned_df.filter(F.col("OrderId") == "O002").first()
        self.assertEqual(order2["TotalAmount"], 500.0)  # 500 * 1
        
        order3 = cleaned_df.filter(F.col("OrderId") == "O003").first()
        self.assertEqual(order3["TotalAmount"], 900.0)  # 300 * 3
        
        # Check that all required fields are not null
        self.assertEqual(cleaned_df.filter(F.col("OrderId").isNull()).count(), 0)
        self.assertEqual(cleaned_df.filter(F.col("ItemName").isNull()).count(), 0)
        self.assertEqual(cleaned_df.filter(F.col("PricePerUnit").isNull()).count(), 0)
        self.assertEqual(cleaned_df.filter(F.col("Qty").isNull()).count(), 0)
        self.assertEqual(cleaned_df.filter(F.col("Date").isNull()).count(), 0)
        self.assertEqual(cleaned_df.filter(F.col("CustId").isNull()).count(), 0)
        
        # Check that "Null" string values are removed
        self.assertEqual(cleaned_df.filter(F.col("ItemName") == "Null").count(), 0)

if __name__ == "__main__":
    unittest.main()