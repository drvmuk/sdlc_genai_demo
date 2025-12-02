import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import os
import tempfile
from src.data_processing import clean_data

class TestDataProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDataProcessing") \
            .master("local[1]") \
            .getOrCreate()
        
        # Create test data
        # Customer data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),  # Duplicate
            ("C004", None, "alice@example.com", "West"),  # Contains null
            ("C005", "Charlie Brown", "charlie@example.com", "North")
        ]
        
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, schema=customer_schema)
        
        # Order data
        order_data = [
            ("O001", "Laptop", 1000.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Phone", 500.0, 1, datetime.date(2023, 2, 20), "C002"),
            ("O003", "Tablet", 300.0, 3, datetime.date(2023, 3, 10), "C003"),
            ("O003", "Tablet", 300.0, 3, datetime.date(2023, 3, 10), "C003"),  # Duplicate
            ("O004", "Monitor", None, 2, datetime.date(2023, 4, 5), "C001"),  # Contains null
            ("O005", "Keyboard", 50.0, 5, datetime.date(2023, 5, 12), "C005")
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
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    def test_clean_data(self):
        # Test the clean_data function
        cleaned_customer_df, cleaned_order_df = clean_data(self.customer_df, self.order_df)
        
        # Check customer data cleaning
        self.assertEqual(cleaned_customer_df.count(), 4)  # Should have removed duplicates and nulls
        
        # Check order data cleaning
        self.assertEqual(cleaned_order_df.count(), 4)  # Should have removed duplicates and nulls
        
        # Check TotalAmount calculation
        order_with_total = cleaned_order_df.filter(cleaned_order_df.OrderId == "O001").collect()[0]
        self.assertEqual(order_with_total.TotalAmount, 2000.0)  # 1000.0 * 2
        
        # Verify no nulls in cleaned data
        self.assertEqual(cleaned_customer_df.filter(cleaned_customer_df.Name.isNull()).count(), 0)
        self.assertEqual(cleaned_order_df.filter(cleaned_order_df.PricePerUnit.isNull()).count(), 0)

if __name__ == "__main__":
    unittest.main()