"""
Unit tests for the data_loader module.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

from src.data_loader import load_csv_to_delta, load_customer_data, load_order_data

class TestDataLoader(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("TestDataLoader") \
            .master("local[1]") \
            .getOrCreate()
        
        # Define sample data
        cls.customer_data = [
            {"CustId": "C001", "CustomerName": "John Doe", "CustomerAddress": "123 Main St", "CustomerEmail": "john@example.com"},
            {"CustId": "C002", "CustomerName": "Jane Smith", "CustomerAddress": "456 Oak Ave", "CustomerEmail": "jane@example.com"}
        ]
        
        cls.order_data = [
            {"OrderId": "O001", "CustId": "C001", "OrderDate": "2023-01-15", "TotalAmount": 125.50, "OrderStatus": "Completed"},
            {"OrderId": "O002", "CustId": "C002", "OrderDate": "2023-01-20", "TotalAmount": 75.25, "OrderStatus": "Processing"}
        ]
        
        # Convert to Spark DataFrames
        cls.customer_schema = StructType([
            StructField("CustId", StringType(), False),
            StructField("CustomerName", StringType(), True),
            StructField("CustomerAddress", StringType(), True),
            StructField("CustomerEmail", StringType(), True)
        ])
        
        cls.order_schema = StructType([
            StructField("OrderId", StringType(), False),
            StructField("CustId", StringType(), False),
            StructField("OrderDate", StringType(), True),
            StructField("TotalAmount", DoubleType(), True),
            StructField("OrderStatus", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(cls.customer_data, cls.customer_schema)
        cls.order_df = cls.spark.createDataFrame(cls.order_data, cls.order_schema)
    
    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()
    
    @patch('src.data_loader.spark')
    def test_load_csv_to_delta(self, mock_spark):
        # Setup mock
        mock_spark.read.option.return_value.option.return_value.csv.return_value = self.customer_df
        mock_spark.read.option.return_value.option.return_value.csv.return_value.withColumn.return_value = self.customer_df
        
        # Call the function
        result =