"""
Unit tests for the SCD Type 2 loader module.
"""
import unittest
from unittest.mock import patch, MagicMock
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd

from src.scd_type2_loader import load_csv_to_delta, create_scd_type2_table

class TestScdType2Loader(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("SCD Type 2 Loader Test") \
            .master("local[1]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Sample customer data
        customer_data = [
            {"CustId": "C001", "Name": "John Doe", "Email": "john@example.com", "Address": "123 Main St"},
            {"CustId": "C002", "Name": "Jane Smith", "Email": "jane@example.com", "Address": "456 Oak Ave"},
            {"CustId": "C003", "Name": "Bob Johnson", "Email": "bob@example.com", "Address": "789 Pine Rd"},
            {"CustId": "C004", "Name": "Alice Brown", "Email": None, "Address": "101 Maple Dr"}  # Null value
        ]
        cls.customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("Address", StringType(), True)
        ])
        cls.customer_df = cls.spark.createDataFrame(pd.DataFrame(customer_data), schema=cls.customer_schema)
        
        # Sample order data
        order_data = [
            {"OrderId": "O001", "CustId": "C001", "Amount": 100, "OrderDate": "2023-01-15"},
            {"OrderId": "O002", "CustId": "C002", "Amount": 200, "OrderDate": "2023-01-20"},
            {"OrderId": "O003", "CustId": "C001", "Amount": 150, "OrderDate": "2023-02-10"},
            {"OrderId": "O004", "CustId": "C003", "Amount": 300, "OrderDate": "2023-02-15"}
        ]
        cls.order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("CustId", StringType(), True),
            StructField("Amount", IntegerType(), True),
            StructField("OrderDate", StringType(), True)
        ])
        cls.order_df = cls.spark.createDataFrame(pd.DataFrame(order_data), schema=cls.order_schema)
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    @patch('src.scd_type2_loader.logger')
    def test_load_csv_to_delta(self, mock_logger):
        """Test loading CSV data to Delta table with null and duplicate removal"""
        # Add a duplicate record to test duplicate removal
        customer_data_with_duplicate = self.customer_df.union(
            self.spark.createDataFrame([{"CustId": "C001", "Name": "John Doe", "Email": "john@example.com", "Address": "123 Main St"}], 
                                      schema=self.customer_schema)
        )
        
        with patch('src.scd_type2_loader.spark.read.option') as mock_read:
            mock_read.return_value.option.return_value.csv.return_value = customer_data_with_duplicate
            with patch('pyspark.sql.DataFrame.write') as mock_write:
                mock_write.return_value.format.return_value.mode.return_value.saveAsTable = MagicMock()
                
                result_df = load_csv_to_delta(self.spark, "mock_path", "mock_table")
                
                # Verify the result doesn't have nulls or duplicates
                self.assertEqual(result_df.count(), 3)  # 4 original - 1 null - 0 duplicates = 3
                
                # Verify logging calls
                mock_logger.info.assert_any_call("Successfully loaded data to mock_table")
    
    @patch('src.scd_type2_loader.logger')
    def test_create_scd_type2_table(self, mock_logger):
        """Test creating SCD Type 2 table by joining customer and order data"""
        # Clean customer data (remove nulls)
        clean_customer_df = self.customer_df.dropna()
        
        with patch('pyspark.sql.DataFrame.write') as mock_write:
            mock_write.return_value.format.return_value.mode.return_value.saveAsTable = MagicMock()
            
            result_df = create_scd_type2_table(self.spark, clean_customer_df, self.order_df)
            
            # Verify SCD Type 2 columns are added
            self.assertTrue("effective_from" in result_df.columns)
            self.assertTrue("effective_to" in result_df.columns)
            self.assertTrue("is_current" in result_df.columns)
            
            # Verify only matching records are included (inner join)
            self.assertEqual(result_df.count(), 3)  # 3 orders match with customers
            
            # Verify all records are marked as current
            self.assertEqual(result_df.filter("is_current = true").count(), 3)
            
            # Verify logging calls
            mock_logger.info.assert_any_call("Successfully created SCD Type 2 table: gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

if __name__ == '__main__':
    unittest.main()