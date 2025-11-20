import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import tempfile
import os
import shutil
from src.data_processing import clean_and_transform_data

class TestDataProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDataProcessing") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create temporary directory for test data
        cls.temp_dir = tempfile.mkdtemp()
        cls.customer_path = os.path.join(cls.temp_dir, "customer")
        cls.order_path = os.path.join(cls.temp_dir, "order")
        
        # Create sample data
        # Customer data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C004", "Alice Brown", "alice@example.com", "West"),
            ("C005", None, "invalid@example.com", "North"),  # Null value
            ("C001", "John Doe", "john@example.com", "North")  # Duplicate
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
            ("O002", "Phone", 500.0, 1, datetime.date(2023, 2, 10), "C002"),
            ("O003", "Tablet", 300.0, 3, datetime.date(2023, 3, 5), "C003"),
            ("O004", "Monitor", 200.0, 2, datetime.date(2023, 4, 20), "C004"),
            ("O005", "Keyboard", 50.0, None, datetime.date(2023, 5, 12), "C001"),  # Null value
            ("O001", "Laptop", 1000.0, 2, datetime.date(2023, 1, 15), "C001")  # Duplicate
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
        # Stop Spark session
        cls.spark.stop()
        
        # Clean up temporary directory
        shutil.rmtree(cls.temp_dir)
    
    def test_clean_and_transform_data(self):
        # Test the clean_and_transform_data function
        clean_customer_df, clean_order_df = clean_and_transform_data(self.customer_df, self.order_df)
        
        # Check that nulls are removed from customer data
        self.assertEqual(clean_customer_df.count(), 4)
        
        # Check that duplicates are removed from customer data
        self.assertEqual(clean_customer_df.filter(clean_customer_df.CustId == "C001").count(), 1)
        
        # Check that nulls are removed from order data
        self.assertEqual(clean_order_df.count(), 4)
        
        # Check that duplicates are removed from order data
        self.assertEqual(clean_order_df.filter(clean_order_df.OrderId == "O001").count(), 1)
        
        # Check that TotalAmount column is added and calculated correctly
        order_with_laptop = clean_order_df.filter(clean_order_df.ItemName == "Laptop").collect()[0]
        self.assertEqual(order_with_laptop.TotalAmount, 2000.0)  # 1000.0 * 2

if __name__ == "__main__":
    unittest.main()