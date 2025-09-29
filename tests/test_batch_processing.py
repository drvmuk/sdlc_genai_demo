import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import pandas as pd
from src.batch_processing import (
    clean_customer_data,
    clean_order_data,
    create_spark_session
)

class TestBatchProcessing(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = (SparkSession.builder
                    .appName("TestCustomerOrderProcessing")
                    .master("local[*]")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .getOrCreate())
        
        # Sample customer data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C004", "Alice Brown", "alice@example.com", "West"),
            ("C005", None, "invalid@example.com", "North"),
            ("C006", "Duplicate User", "dup@example.com", "South"),
            ("C006", "Duplicate User", "dup@example.com", "South")
        ]
        
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, schema=customer_schema)
        
        # Sample order data
        today = datetime.date.today()
        order_data = [
            ("O001", "Laptop", 1000.0, 1, today, "C001"),
            ("O002", "Phone", 500.0, 2, today, "C002"),
            ("O003", "Tablet", 300.0, 3, today, "C003"),
            ("O004", "Monitor", 200.0, 2, today, "C004"),
            ("O005", "Keyboard", 50.0, None, today, "C001"),
            ("O006", "Mouse", 25.0, 2, today, None),
            ("O007", "Headphones", 100.0, 1, today, "C002"),
            ("O007", "Headphones", 100.0, 1, today, "C002")  # Duplicate
        ]
        
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        cls.order_df = cls.spark.createDataFrame(order_data, schema=order_schema)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_clean_customer_data(self):
        """Test cleaning customer data by removing nulls and duplicates"""
        result = clean_customer_data(self.customer_df)
        
        # Check row count (should be 4 after removing nulls and duplicates)
        self.assertEqual(result.count(), 4)
        
        # Check that all required fields have values
        self.assertEqual(result.filter("CustId IS NULL OR Name IS NULL OR EmailId IS NULL OR Region IS NULL").count(), 0)
        
        # Check that duplicates were removed
        self.assertEqual(result.select("CustId").distinct().count(), result.count())
    
    def test_clean_order_data(self):
        """Test cleaning order data and adding TotalAmount column"""
        result = clean_order_data(self.order_df)
        
        # Check row count (should be 4 after removing nulls and duplicates)
        self.assertEqual(result.count(), 4)
        
        # Check that all required fields have values
        self.assertEqual(result.filter(
            "OrderId IS NULL OR ItemName IS NULL OR PricePerUnit IS NULL OR " +
            "Qty IS NULL OR Date IS NULL OR CustId IS NULL"
        ).count(), 0)
        
        # Check that TotalAmount column was added correctly
        order_o001 = result.filter("OrderId = 'O001'").collect()[0]
        self.assertEqual(order_o001["TotalAmount"], order_o001["PricePerUnit"] * order_o001["Qty"])
        
        # Check that duplicates were removed
        self.assertEqual(result.select("OrderId").distinct().count(), result.count())

if __name__ == "__main__":
    unittest.main()