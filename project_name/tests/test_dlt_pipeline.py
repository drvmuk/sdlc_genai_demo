import unittest
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from unittest.mock import patch, MagicMock

class TestDLTPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDLTPipeline") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create sample data
        # Customer data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C004", "Alice Brown", "alice@example.com", "West"),
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
    
    @patch('dlt.read')
    def test_order_silver_transformation(self, mock_dlt_read):
        # Import the function to test
        from src.dlt_pipeline import order_silver
        
        # Mock the DLT read function to return our test data
        mock_dlt_read.return_value = self.order_df
        
        # Call the function
        result = order_silver()
        
        # Check that TotalAmount is calculated correctly
        laptop_row = result.filter(result.ItemName == "Laptop").collect()[0]
        self.assertEqual(laptop_row.TotalAmount, 2000.0)  # 1000.0 * 2
        
        phone_row = result.filter(result.ItemName == "Phone").collect()[0]
        self.assertEqual(phone_row.TotalAmount, 500.0)  # 500.0 * 1
        
        # Check that all rows are present
        self.assertEqual(result.count(), 4)
    
    @patch('dlt.read')
    def test_customer_silver_transformation(self, mock_dlt_read):
        # Import the function to test
        from src.dlt_pipeline import customer_silver
        
        # Create test data with nulls and duplicates
        test_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", None, "bob@example.com", "East"),  # Null name
            ("C001", "John Doe", "john@example.com", "North")  # Duplicate
        ]
        
        schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        test_df = self.spark.createDataFrame(test_data, schema=schema)
        
        # Mock the DLT read function to return our test data
        mock_dlt_read.return_value = test_df
        
        # Call the function
        result = customer_silver()
        
        # Check that nulls are removed
        self.assertEqual(result.filter(result.Name.isNull()).count(), 0)
        
        # Check that duplicates are removed
        self.assertEqual(result.count(), 2)
        
        # Check that the correct data is preserved
        self.assertEqual(result.filter(result.CustId == "C001").count(), 1)
        self.assertEqual(result.filter(result.CustId == "C002").count(), 1)

if __name__ == "__main__":
    unittest.main()