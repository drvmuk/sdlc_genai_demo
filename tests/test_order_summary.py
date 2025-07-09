"""
Unit tests for order summary module.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType
import os
from datetime import datetime, date

# Import the module to test
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.order_summary import create_order_summary, update_order_summary_on_customer_change

class TestOrderSummary(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestOrderSummary") \
            .master("local[2]") \
            .getOrCreate()
        
        # Create test catalog and schema
        cls.catalog = "test_catalog"
        cls.schema = "test_schema"
        
        # Create test data
        cls.create_test_data()
    
    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()
    
    @classmethod
    def create_test_data(cls):
        # Create test catalog and schema
        cls.spark.sql(f"CREATE DATABASE IF NOT EXISTS {cls.catalog}.{cls.schema}")
        
        # Create customer test data
        customer_data = [
            (1, "John Doe", "123 Main St", "555-1234", datetime.now()),
            (2, "Jane Smith", "456 Oak Ave", "555-5678", datetime.now()),
            (3, "Bob Johnson", "789 Pine Rd", "555-9012", datetime.now())
        ]
        
        customer_schema = StructType([
            StructField("CustId", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("Phone", StringType(), True),
            StructField("LoadTimestamp", TimestampType(), True)
        ])
        
        customer_df = cls.spark.createDataFrame(customer_data, customer_schema)
        customer_df.write.format("delta").mode("overwrite").saveAsTable(f"{cls.catalog}.{cls.schema}.customer")
        
        # Create order test data
        today = datetime.now().strftime("%Y-%m-%d")
        order_data = [
            (101, 1, today, 100.50, "Completed", datetime.now()),
            (102, 2, today, 75.25, "Pending", datetime.now()),
            (103, 3, today, 200.00, "Completed", datetime.now()),
            (104, 1, today, 50.75, "Completed", datetime.now())
        ]
        
        order_schema = StructType([
            StructField("OrderId", IntegerType(), True),
            StructField("CustId", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("TotalAmount", DoubleType(), True),
            StructField("Status", StringType(), True),
            StructField("LoadTimestamp", TimestampType(), True)
        ])
        
        order_df = cls.spark.createDataFrame(order_data, order_schema)
        order_df.write.format("delta").mode("overwrite").saveAsTable(f"{cls.catalog}.{cls.schema}.order")
    
    def test_create_order_summary(self):
        """Test creating order summary table"""
        # Run the function
        create_order_summary(self.spark, self.catalog, self.schema)
        
        # Verify the table was created
        tables = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{self.schema}").filter("tableName = 'ordersummary'")
        self.assertEqual(tables.count(), 1)
        
        # Verify data was loaded correctly
        order_summary_df = self.spark.table(f"{self.catalog}.{self.schema}.ordersummary")
        
        # Should have 4 rows (one for each order)
        self.assertEqual(order_summary_df.count(), 4)
        
        # Check SCD Type 2 columns exist
        self.assertTrue("StartDate" in order_summary_df.columns)
        self.assertTrue("EndDate" in order_summary_df.columns)
        self.assertTrue("IsActive" in order_summary_df.columns)
        
        # All records should be active
        active_count = order_summary_df.filter("IsActive = true").count()
        self.assertEqual(active_count, 4)
    
    def test_update_order_summary_on_customer_change(self):
        """Test updating order summary when customer data changes"""
        # First, create the order summary table
        create_order_summary(self.spark, self.catalog, self.schema)
        
        # Update a customer record
        self.spark.sql(f"""
            UPDATE {self.catalog}.{self.schema}.customer
            SET Name = 'John Doe Updated', Phone = '555-9999'
            WHERE CustId = 1
        """)
        
        # Run the update function
        update_order_summary_on_customer_change(self.spark, self.catalog, self.schema)
        
        # Verify the updates
        order_summary_df = self.spark.table(f"{self.catalog}.{self.schema}.ordersummary")
        
        # Should have 6 rows now (4 original + 2 updated for customer 1's orders)
        self.assertEqual(order_summary_df.count(), 6)
        
        # Check that we have 2 inactive records for customer 1
        inactive_records = order_summary_df.filter("CustId = 1 AND IsActive = false").count()
        self.assertEqual(inactive_records, 2)
        
        # Check that we have 2 active records for customer 1 with updated name
        updated_records = order_summary_df.filter("CustId = 1 AND IsActive = true AND Name = 'John Doe Updated'").count()
        self.assertEqual(updated_records, 2)

if __name__ == "__main__":
    unittest.main()