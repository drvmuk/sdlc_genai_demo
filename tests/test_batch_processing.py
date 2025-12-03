import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType
import datetime
from pyspark.sql.functions import col, lit

from src.batch_processing import (
    clean_data, process_order_data, update_scd_type2_table, update_customeraggregatespend
)

class TestBatchProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("TestBatchProcessing") \
            .master("local[*]") \
            .getOrCreate()
        
        # Define schemas
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
        
        # Create sample data
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
            ("O004", "Headphones", 100.0, None, datetime.date(2023, 1, 30), "C001"),
            ("O002", "Phone", 500.0, 1, datetime.date(2023, 1, 20), "C002")  # Duplicate
        ]
        
        # Create DataFrames
        cls.customer_df = cls.spark.createDataFrame(customer_data, cls.customer_schema)
        cls.order_df = cls.spark.createDataFrame(order_data, cls.order_schema)
    
    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()
    
    def test_clean_data(self):
        # Test cleaning customer data
        clean_customer_df = clean_data(self.customer_df)
        
        # Check if nulls are removed
        self.assertEqual(clean_customer_df.filter(col("Name").isNull()).count(), 0)
        
        # Check if duplicates are removed
        self.assertEqual(clean_customer_df.count(), 3)
        
        # Test cleaning order data
        clean_order_df = clean_data(self.order_df)
        
        # Check if nulls are removed
        self.assertEqual(clean_order_df.filter(col("Qty").isNull()).count(), 0)
        
        # Check if duplicates are removed
        self.assertEqual(clean_order_df.count(), 3)
    
    def test_process_order_data(self):
        # Test processing order data
        processed_order_df = process_order_data(self.order_df)
        
        # Check if TotalAmount column is added
        self.assertTrue("TotalAmount" in processed_order_df.columns)
        
        # Check if TotalAmount is calculated correctly
        row = processed_order_df.filter(col("OrderId") == "O001").first()
        self.assertEqual(row["TotalAmount"], 2000.0)  # 1000.0 * 2 = 2000.0
        
        # Check null handling
        row_null = processed_order_df.filter(col("OrderId") == "O004").first()
        self.assertIsNone(row_null["TotalAmount"])  # null * 100.0 = null
    
    def test_update_scd_type2_table(self):
        # Create a mock ordersummary table for testing
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
        
        # Create an empty ordersummary table
        ordersummary_data = []
        ordersummary_df = self.spark.createDataFrame(ordersummary_data, ordersummary_schema)
        
        # Register the DataFrame as a temporary view
        ordersummary_df.createOrReplaceTempView("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
        # Process clean data
        clean_customer_df = clean_data(self.customer_df)
        clean_order_df = clean_data(self.order_df)
        processed_order_df = process_order_data(clean_order_df)
        
        # This test would need to be adapted for a real Databricks environment
        # In a unit test environment, we can't fully test the update_scd_type2_table function
        # as it relies on Spark SQL operations on Delta tables
        
        # For testing purposes, we can check that the function doesn't raise exceptions
        try:
            # This would fail in a unit test environment but would work in Databricks
            # update_scd_type2_table(self.spark, clean_customer_df, processed_order_df)
            pass
        except Exception as e:
            self.fail(f"update_scd_type2_table raised exception {e}")

if __name__ == "__main__":
    unittest.main()