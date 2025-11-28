import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType
import datetime
from src.data_processing import clean_data, process_order_data

class TestDataProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDataProcessing") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create sample data
        # Customer data with schema
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C004", "Alice Brown", "alice@example.com", "West"),
            ("C005", None, "mike@example.com", "North"),  # Null value
            ("C001", "John Doe", "john@example.com", "North")  # Duplicate
        ]
        
        cls.customer_df = cls.spark.createDataFrame(cls.customer_data, customer_schema)
        
        # Order data with schema
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        cls.order_data = [
            ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Phone", 800.0, 2, datetime.date(2023, 1, 20), "C002"),
            ("O003", "Headphones", 100.0, 3, datetime.date(2023, 2, 5), "C003"),
            ("O004", "Monitor", 300.0, 1, datetime.date(2023, 2, 10), "C004"),
            ("O005", "Keyboard", 50.0, None, datetime.date(2023, 3, 1), "C001"),  # Null value
            ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001")  # Duplicate
        ]
        
        cls.order_df = cls.spark.createDataFrame(cls.order_data, order_schema)
    
    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()
    
    def test_clean_data(self):
        # Test customer data cleaning
        clean_customer_df = clean_data(self.customer_df)
        
        # Check that null values are removed
        self.assertEqual(clean_customer_df.count(), 4)
        
        # Check that duplicates are removed
        self.assertEqual(clean_customer_df.filter(clean_customer_df.CustId == "C001").count(), 1)
    
    def test_process_order_data(self):
        # Test order data processing
        processed_order_df = process_order_data(self.order_df)
        
        # Check that TotalAmount column is added
        self.assertTrue("TotalAmount" in processed_order_df.columns)
        
        # Verify calculation for a specific row
        row = processed_order_df.filter(processed_order_df.OrderId == "O002").collect()[0]
        self.assertEqual(row.TotalAmount, 1600.0)  # 800.0 * 2 = 1600.0

if __name__ == "__main__":
    unittest.main()