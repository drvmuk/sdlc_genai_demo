import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime

# Mock the dlt module since it's only available in Databricks
sys.modules['dlt'] = MagicMock()
import dlt

class TestDeltaLiveTables(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDeltaLiveTables") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Make the spark session available globally for the module
        import builtins
        builtins.spark = cls.spark
        
        # Define test data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "East"),
            ("C002", "Jane Smith", "jane@example.com", "West"),
        ]
        
        order_data = [
            ("O001", "Product A", 10.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Product B", 15.0, 1, datetime.date(2023, 1, 20), "C002"),
        ]
        
        # Create DataFrames
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, schema=customer_schema)
        cls.order_df = cls.spark.createDataFrame(order_data, schema=order_schema)
        
        # Import the module under test
        from src import delta_live_tables
        cls.dlt_module = delta_live_tables
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    @patch('src.delta_live_tables.dlt.read')
    def test_order_silver(self, mock_dlt_read):
        # Mock the dlt.read function to return our test DataFrame
        mock_dlt_read.return_value = self.order_df
        
        # Call the function under test
        result_df = self.dlt_module.order_silver()
        
        # Check that TotalAmount column was added and calculated correctly
        self.assertTrue("TotalAmount" in result_df.columns)
        
        # Check the values
        result_rows = result_df.collect()
        self.assertEqual(result_rows[0]["TotalAmount"], 20.0)  # 10.0 * 2
        self.assertEqual(result_rows[1]["TotalAmount"], 15.0)  # 15.0 * 1
    
    @patch('src.delta_live_tables.dlt.read')
    def test_customer_silver(self, mock_dlt_read):
        # Mock the dlt.read function to return our test DataFrame
        mock_dlt_read.return_value = self.customer_df
        
        # Call the function under test
        result_df = self.dlt_module.customer_silver()
        
        # Check the row count
        self.assertEqual(result_df.count(), 2)
        
        # Verify the data is as expected
        result = result_df.filter("CustId = 'C001'").collect()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["Name"], "John Doe")

if __name__ == "__main__":
    unittest.main()