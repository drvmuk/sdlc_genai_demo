"""
Tests for batch processing module.
"""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
from src.batch_processing import (
    clean_data,
    process_order_data
)


class TestBatchProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestBatchProcessing") \
            .master("local[1]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    def test_clean_data(self):
        # Create a test DataFrame with nulls and duplicates
        data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", None, "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C001", "John Doe", "john@example.com", "North"),  # Duplicate
            (None, "Alice Brown", "alice@example.com", "West")
        ]
        
        schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        
        # Clean the data
        cleaned_df = clean_data(df)
        
        # Check that nulls are removed
        self.assertEqual(cleaned_df.filter("CustId IS NULL OR EmailId IS NULL").count(), 0)
        
        # Check that duplicates are removed
        self.assertEqual(cleaned_df.count(), 3)
    
    def test_process_order_data(self):
        # Create a test DataFrame for orders
        data = [
            ("O001", "Item1", 10.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Item2", 15.0, 3, datetime.date(2023, 1, 16), "C002"),
            ("O003", "Item3", 20.0, 1, datetime.date(2023, 1, 17), "C003")
        ]
        
        schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        
        # Process the order data
        processed_df = process_order_data(df)
        
        # Check that TotalAmount column is added and calculated correctly
        total_amounts = [row.TotalAmount for row in processed_df.collect()]
        expected_amounts = [20.0, 45.0, 20.0]
        
        self.assertEqual(total_amounts, expected_amounts)


if __name__ == "__main__":
    unittest.main()