import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import os
import tempfile
from src.batch_processing import clean_customer_data, clean_order_data

class TestBatchProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestBatchProcessing") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Define test data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "East"),
            ("C002", "Jane Smith", "jane@example.com", "West"),
            ("C003", None, "bob@example.com", "North"),
            ("C004", "Alice Brown", None, "South"),
            ("C001", "John Doe", "john@example.com", "East")  # Duplicate
        ]
        
        order_data = [
            ("O001", "Product A", 10.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Product B", 15.0, 1, datetime.date(2023, 1, 20), "C002"),
            ("O003", "Product C", 5.0, 3, datetime.date(2023, 1, 25), "C001"),
            ("O004", None, 20.0, 1, datetime.date(2023, 1, 30), "C002"),
            ("O005", "Product E", 25.0, None, datetime.date(2023, 2, 5), "C003"),
            ("O001", "Product A", 10.0, 2, datetime.date(2023, 1, 15), "C001")  # Duplicate
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
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    def test_clean_customer_data(self):
        # Test customer data cleaning
        cleaned_customer = clean_customer_data(self.customer_df)
        
        # Check row count (should remove nulls and duplicates)
        self.assertEqual(cleaned_customer.count(), 2)
        
        # Check that all required fields are not null
        null_counts = cleaned_customer.select([
            sum(col.isNull().cast("int")).alias(col_name)
            for col_name, col in cleaned_customer.dtypes
        ]).collect()[0]
        
        for count in null_counts:
            self.assertEqual(count, 0)
    
    def test_clean_order_data(self):
        # Test order data cleaning
        cleaned_order = clean_order_data(self.order_df)
        
        # Check row count (should remove nulls and duplicates)
        self.assertEqual(cleaned_order.count(), 2)
        
        # Check that TotalAmount is calculated correctly
        order1 = cleaned_order.filter("OrderId = 'O001'").collect()[0]
        self.assertEqual(order1["TotalAmount"], 20.0)  # 10.0 * 2
        
        order2 = cleaned_order.filter("OrderId = 'O002'").collect()[0]
        self.assertEqual(order2["TotalAmount"], 15.0)  # 15.0 * 1

if __name__ == "__main__":
    unittest.main()