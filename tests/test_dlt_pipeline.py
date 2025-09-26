import unittest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType
import datetime
import sys
import os
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

# Mock DLT functions
class MockDLT:
    @staticmethod
    def table(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    @staticmethod
    def read(table_name):
        if table_name == "customer_bronze":
            return customer_bronze_data
        elif table_name == "order_bronze":
            return order_bronze_data
        elif table_name == "customer_silver":
            return customer_silver_data
        elif table_name == "order_silver":
            return order_silver_data
        elif table_name == "ordersummary":
            return ordersummary_data
        else:
            raise ValueError(f"Unknown table: {table_name}")

# Create mock data
customer_bronze_data = None
order_bronze_data = None
customer_silver_data = None
order_silver_data = None
ordersummary_data = None

# Mock the dlt module
sys.modules['dlt'] = MockDLT()

# Now import the module with mocked dependencies
from dlt_pipeline import customer_silver, order_silver, ordersummary, customeraggregatespend

class TestDLTPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestDLTPipeline") \
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
        
        # Create DataFrames
        global customer_bronze_data, order_bronze_data
        customer_bronze_data = cls.spark.createDataFrame(customer_data, cls.customer_schema)
        order_bronze_data = cls.spark.createDataFrame(order_data, cls.order_schema)
        
        # Create silver data
        global customer_silver_data, order_silver_data
        customer_silver_data = customer_bronze_data.dropna().dropDuplicates()
        order_silver_data = (
            order_bronze_data
            .dropna()
            .dropDuplicates()
            .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        )
        
        # Create ordersummary data
        global ordersummary_data
        ordersummary_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("TotalAmount", DoubleType(), True),
            StructField("IsActive", BooleanType(), True),
            StructField("StartDate", TimestampType(), True),
            StructField("EndDate", TimestampType(), True)
        ])
        
        ordersummary_data = cls.spark.createDataFrame([], ordersummary_schema)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    @patch('dlt_pipeline.dlt')
    def test_customer_silver(self, mock_dlt):
        mock_dlt.read.return_value = customer_bronze_data
        
        result = customer_silver()
        
        # Check nulls and duplicates are removed
        self.assertEqual(result.count(), 3)
        self.assertEqual(result.filter(F.col("CustId") == "C001").count(), 1)
    
    @patch('dlt_pipeline.dlt')
    def test_order_silver(self, mock_dlt):
        mock_dlt.read.return_value = order_bronze_data
        
        result = order_silver()
        
        # Check nulls and duplicates are removed and TotalAmount added
        self.assertEqual(result.count(), 3)
        self.assertTrue("TotalAmount" in result.columns)
        self.assertEqual(result.filter(F.col("OrderId") == "O001").first()["TotalAmount"], 2000.0)
    
    @patch('dlt_pipeline.dlt')
    def test_customeraggregatespend(self, mock_dlt):
        # Create mock ordersummary data for this test
        ordersummary_test_data = [
            ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1000.0, 2, 
             datetime.date(2023, 1, 15), 2000.0, True, datetime.datetime.now(), None),
            ("C001", "John Doe", "john@example.com", "North", "O004", "Monitor", 200.0, 2, 
             datetime.date(2023, 1, 15), 400.0, True, datetime.datetime.now(), None),
            ("C002", "Jane Smith", "jane@example.com", "South", "O002", "Phone", 500.0, 1, 
             datetime.date(2023, 1, 20), 500.0, True, datetime.datetime.now(), None)
        ]
        
        mock_ordersummary = self.spark.createDataFrame(ordersummary_test_data, ordersummary_data.schema)
        mock_dlt.read.return_value = mock_ordersummary
        
        result = customeraggregatespend()
        
        # Check aggregation
        self.assertEqual(result.count(), 2)  # 2 unique date-customer combinations
        
        # Check John's total spend on 2023-01-15
        john_spend = result.filter(F.col("Name") == "John Doe").first()["TotalAmount"]
        self.assertEqual(john_spend, 2400.0)  # 2000 + 400

if __name__ == "__main__":
    unittest.main()