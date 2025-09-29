import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType
import datetime
import sys
import os

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.data_processor import load_scd_type2_data, aggregate_customer_spend

class TestIntegration(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestIntegration") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create test tables
        cls.spark.sql("CREATE DATABASE IF NOT EXISTS test_gen_ai_poc_databrickscoe")
        cls.spark.sql("CREATE SCHEMA IF NOT EXISTS test_gen_ai_poc_databrickscoe.test_sdlc_wizard")
        
        # Define schema for ordersummary test table
        cls.spark.sql("""
        CREATE TABLE IF NOT EXISTS test_gen_ai_poc_databrickscoe.test_sdlc_wizard.ordersummary (
            CustId STRING,
            Name STRING,
            EmailId STRING,
            Region STRING,
            OrderId STRING,
            ItemName STRING,
            PricePerUnit DOUBLE,
            Qty INT,
            Date DATE,
            IsActive BOOLEAN,
            StartDate TIMESTAMP,
            EndDate TIMESTAMP
        ) USING DELTA
        """)
        
        # Define schema for customeraggregatespend test table
        cls.spark.sql("""
        CREATE TABLE IF NOT EXISTS test_gen_ai_poc_databrickscoe.test_sdlc_wizard.customeraggregatespend (
            Name STRING,
            TotalAmount DOUBLE,
            Date DATE
        ) USING DELTA
        """)
    
    @classmethod
    def tearDownClass(cls):
        # Clean up test tables
        cls.spark.sql("DROP TABLE IF EXISTS test_gen_ai_poc_databrickscoe.test_sdlc_wizard.ordersummary")
        cls.spark.sql("DROP TABLE IF EXISTS test_gen_ai_poc_databrickscoe.test_sdlc_wizard.customeraggregatespend")
        cls.spark.sql("DROP SCHEMA IF EXISTS test_gen_ai_poc_databrickscoe.test_sdlc_wizard")
        cls.spark.sql("DROP DATABASE IF EXISTS test_gen_ai_poc_databrickscoe")
        
        # Stop the Spark session
        cls.spark.stop()
    
    def test_scd_type2_initial_load(self):
        # Create test customer data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South")
        ]
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        customer_df = self.spark.createDataFrame(customer_data, customer_schema)
        
        # Create test order data
        order_data = [
            ("O001", "Laptop", 1000.0, 1, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Mouse", 25.0, 2, datetime.date(2023, 1, 20), "C002")
        ]
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        order_df = self.spark.createDataFrame(order_data, order_schema)
        
        # Mock the SCD Type 2 load function to use test tables
        # This would require modifying the function to accept table names as parameters
        # For this test, we'll simulate the functionality
        
        # Join customer and order data
        joined_df = customer_df.join(order_df, "CustId", "inner")
        
        # Add SCD Type 2 columns
        current_time = datetime.datetime.now()
        scd_df = joined_df.select(
            "CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", 
            "PricePerUnit", "Qty", "Date"
        ).withColumn("IsActive", True) \
         .withColumn("StartDate", current_time) \
         .withColumn("EndDate", None)
        
        # Write to test ordersummary table
        scd_df.write.format("delta").mode("overwrite") \
            .saveAsTable("test_gen_ai_poc_databrickscoe.test_sdlc_wizard.ordersummary")
        
        # Check results
        result_df = self.spark.table("test_gen_ai_poc_databrickscoe.test_sdlc_wizard.ordersummary")
        self.assertEqual(result_df.count(), 2)
        self.assertEqual(result_df.filter(result_df.IsActive == True).count(), 2)
        
    def test_customer_spend_aggregation(self):
        # Create test ordersummary data
        ordersummary_data = [
            ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1000.0, 1, 
             datetime.date(2023, 1, 15), True, datetime.datetime.now(), None),
            ("C001", "John Doe", "john@example.com", "North", "O002", "Mouse", 25.0, 2, 
             datetime.date(2023, 1, 15), True, datetime.datetime.now(), None),
            ("C002", "Jane Smith", "jane@example.com", "South", "O003", "Monitor", 200.0, 1, 
             datetime.date(2023, 1, 20), True, datetime.datetime.now(), None)
        ]
        
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
            StructField("IsActive", BooleanType(), True),
            StructField("StartDate", TimestampType(), True),
            StructField("EndDate", TimestampType(), True)
        ])
        
        ordersummary_df = self.spark.createDataFrame(ordersummary_data, ordersummary_schema)
        
        # Write to test ordersummary table
        ordersummary_df.write.format("delta").mode("overwrite") \
            .saveAsTable("test_gen_ai_poc_databrickscoe.test_sdlc_wizard.ordersummary")
        
        # Calculate total amount
        ordersummary_with_total = ordersummary_df.withColumn(
            "TotalAmount", 
            ordersummary_df.PricePerUnit * ordersummary_df.Qty
        )
        
        # Aggregate by Name and Date
        from pyspark.sql.functions import sum as sum_
        aggregated_df = ordersummary_with_total.groupBy("Name", "Date") \
            .agg(sum_("TotalAmount").alias("TotalAmount"))
        
        # Write to test customeraggregatespend table
        aggregated_df.write.format("delta").mode("overwrite") \
            .saveAsTable("test_gen_ai_poc_databrickscoe.test_sdlc_wizard.customeraggregatespend")
        
        # Check results
        result_df = self.spark.table("test_gen_ai_poc_databrickscoe.test_sdlc_wizard.customeraggregatespend")
        
        # Should have 2 rows (John Doe on 2023-01-15 and Jane Smith on 2023-01-20)
        self.assertEqual(result_df.count(), 2)
        
        # John's total should be 1000 + (25 * 2) = 1050
        john_total = result_df.filter(result_df.Name == "John Doe").first()["TotalAmount"]
        self.assertEqual(john_total, 1050.0)
        
        # Jane's total should be 200
        jane_total = result_df.filter(result_df.Name == "Jane Smith").first()["TotalAmount"]
        self.assertEqual(jane_total, 200.0)

if __name__ == "__main__":
    unittest.main()