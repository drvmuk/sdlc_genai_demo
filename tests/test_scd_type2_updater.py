"""
Unit tests for the SCD Type 2 updater module.
"""
import unittest
from unittest.mock import patch, MagicMock
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
import pandas as pd

from src.scd_type2_updater import update_scd_type2_table

class TestScdType2Updater(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("SCD Type 2 Updater Test") \
            .master("local[1]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Sample customer data (current)
        customer_data = [
            {"CustId": "C001", "Name": "John Doe Updated", "Email": "john.new@example.com", "Address": "123 Main St"},
            {"CustId": "C002", "Name": "Jane Smith", "Email": "jane@example.com", "Address": "456 Oak Ave"},
            {"CustId": "C003", "Name": "Bob Johnson", "Email": "bob@example.com", "Address": "789 Pine Rd"}
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
        
        # Sample ordersummary data (existing SCD Type 2 table)
        ordersummary_data = [
            {"CustId": "C001", "Name": "John Doe", "Email": "john@example.com", "Address": "123 Main St", 
             "OrderId": "O001", "Amount": 100, "OrderDate": "2023-01-15", 
             "effective_from": datetime.datetime(2023, 1, 1), "effective_to": datetime.datetime(9999, 12, 31), "is_current": True},
            {"CustId": "C002", "Name": "Jane Smith", "Email": "jane@example.com", "Address": "456 Oak Ave", 
             "OrderId": "O002", "Amount": 200, "OrderDate": "2023-01-20", 
             "effective_from": datetime.datetime(2023, 1, 1), "effective_to": datetime.datetime(9999, 12, 31), "is_current": True},
            {"CustId": "C003", "Name": "Bob Johnson", "Email": "bob@example.com", "Address": "789 Pine Rd", 
             "OrderId": "O004", "Amount": 300, "OrderDate": "2023-02-15", 
             "effective_from": datetime.datetime(2023, 1, 1), "effective_to": datetime.datetime(9999, 12, 31), "is_current": True}
        ]
        cls.ordersummary_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("OrderId", StringType(), True),
            StructField("Amount", IntegerType(), True),
            StructField("OrderDate", StringType(), True),
            StructField("effective_from", TimestampType(), True),
            StructField("effective_to", TimestampType(), True),
            StructField("is_current", BooleanType(), True)
        ])
        cls.ordersummary_df = cls.spark.createDataFrame(pd.DataFrame(ordersummary_data), schema=cls.ordersummary_schema)
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    @patch('src.scd_type2_updater.logger')
    @patch('src.scd_type2_updater.spark')
    def test_update_scd_type2_table(self, mock_spark, mock_logger):
        """Test updating SCD Type 2 table when customer data changes"""
        # Set up mocks
        mock_spark.table.side_effect = lambda table_name: {
            "gen_ai_poc_databrickscoe.sdlc_wizard.customer": self.customer_df,
            "gen_ai_poc_databrickscoe.sdlc_wizard.order": self.order_df
        }.get(table_name)
        
        # Mock DeltaTable
        mock_delta_table = MagicMock()
        mock_delta_table.toDF.return_value = self.ordersummary_df
        mock_delta_table.alias.return_value.merge.return_value.whenMatchedUpdate.return_value.whenNotMatchedInsertAll.return_value.execute = MagicMock()
        
        with patch('src.scd_type2_updater.DeltaTable') as mock_delta:
            mock_delta.forName.return_value = mock_delta_table
            
            # Call the function
            update_scd_type2_table(mock_spark)
            
            # Verify DeltaTable.forName was called
            mock_delta.forName.assert_called_once_with(mock_spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
            
            # Verify merge operation was called
            mock_delta_table.alias.return_value.merge.assert_called_once()
            
            # Verify logging
            mock_logger.info.assert_any_call("No changes detected in customer data")

if __name__ == '__main__':
    unittest.main()