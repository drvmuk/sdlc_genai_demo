"""
Unit tests for data loading module.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os
import tempfile
import shutil
from datetime import datetime

# Import the module to test
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data_loading import load_customer_data, load_order_data

class TestDataLoading(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDataLoading") \
            .master("local[2]") \
            .getOrCreate()
        
        # Create temporary directories for test data
        cls.temp_dir = tempfile.mkdtemp()
        cls.customer_data_path = os.path.join(cls.temp_dir, "customer_data")
        cls.order_data_path = os.path.join(cls.temp_dir, "order_data")
        
        # Create test catalog and schema
        cls.catalog = "test_catalog"
        cls.schema = "test_schema"
        
        # Create test data
        cls.create_test_data()
    
    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()
        
        # Remove temporary directories
        shutil.rmtree(cls.temp_dir)
    
    @classmethod
    def create_test_data(cls):
        # Create customer test data
        customer_data = [
            (1, "John Doe", "123 Main St", "555-1234"),
            (2, "Jane Smith", "456 Oak Ave", "555-5678"),
            (3, "Bob Johnson", "789 Pine Rd", "555-9012"),
            (4, "Alice Brown", "321 Elm Ln", "555-3456"),
            (5, None, "654 Maple Dr", "555-7890")  # Row with null to test cleaning
        ]
        
        customer_schema = StructType([
            StructField("CustId", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("Phone", StringType(), True)
        ])
        
        customer_df = cls.spark.createDataFrame(customer_data, customer_schema)
        customer_df.write.csv(cls.customer_data_path, header=True, mode="overwrite")
        
        # Create order test data
        order_data = [
            (101, 1, datetime.now().strftime("%Y-%m-%d"), 100.50, "Completed"),
            (102, 2, datetime.now().strftime("%Y-%m-%d"), 75.25, "Pending"),
            (103, 3, datetime.now().strftime("%Y-%m-%d"), 200.00, "Completed"),
            (104, 1, datetime.now().strftime("%Y-%m-%d"), 50.75, "Completed"),
            (105, None, datetime.now().strftime("%Y-%m-%d"), 150.00, "Pending")  # Row with null to test cleaning
        ]
        
        order_schema = StructType([
            StructField("OrderId", IntegerType(), True),
            StructField("CustId", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("TotalAmount", DoubleType(), True),
            StructField("Status", StringType(), True)
        ])
        
        order_df = cls.spark.createDataFrame(order_data, order_schema)
        order_df.write.csv(cls.order_data_path, header=True, mode="overwrite")
        
        # Create test catalog and schema
        cls.spark.sql(f"CREATE DATABASE IF NOT EXISTS {cls.catalog}.{cls.schema}")
    
    def test_load_customer_data(self):
        """Test loading customer data into Delta table"""
        # Run the function
        load_customer_data(self.spark, self.customer_data_path, self.catalog, self.schema)
        
        # Verify the table was created
        tables = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{self.schema}").filter("tableName = 'customer'")
        self.assertEqual(tables.count(), 1)
        
        # Verify data was loaded correctly
        customer_df = self.spark.table(f"{self.catalog}.{self.schema}.customer")
        
        # Should have 4 rows (one row had NULL value and should be dropped)
        self.assertEqual(customer_df.count(), 4)
        
        # Check schema includes LoadTimestamp
        self.assertTrue("LoadTimestamp" in customer_df.columns)
    
    def test_load_order_data(self):
        """Test loading order data into Delta table"""
        # Run the function
        load_order_data(self.spark, self.order_data_path, self.catalog, self.schema)
        
        # Verify the table was created
        tables = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{self.schema}").filter("tableName = 'order'")
        self.assertEqual(tables.count(), 1)
        
        # Verify data was loaded correctly
        order_df = self.spark.table(f"{self.catalog}.{self.schema}.order")
        
        # Should have 4 rows (one row had NULL value and should be dropped)
        self.assertEqual(order_df.count(), 4)
        
        # Check schema includes LoadTimestamp
        self.assertTrue("LoadTimestamp" in order_df.columns)

if __name__ == "__main__":
    unittest.main()