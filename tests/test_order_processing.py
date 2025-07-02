"""
Unit tests for the order processing module.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import datetime
from src.order_processing import OrderProcessor

class TestOrderProcessor(unittest.TestCase):
    """Test cases for OrderProcessor class."""

    @classmethod
    def setUpClass(cls):
        """Set up SparkSession and test data."""
        cls.spark = SparkSession.builder \
            .appName("TestOrderProcessor") \
            .master("local[2]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create test customer data
        customer_data = [
            ("C001", "John Doe", "123 Main St", "555-1234"),
            ("C002", "Jane Smith", "456 Oak Ave", "555-5678"),
            ("C003", "Bob Johnson", "789 Pine Rd", "555-9012"),
            ("C004", None, "101 Elm St", "555-3456"),  # Null name
            ("C001", "John Doe", "123 Main St", "555-1234")  # Duplicate
        ]
        
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("CustomerName", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("Phone", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, schema=customer_schema)
        
        # Create test order data
        order_data = [
            ("O001", "C001", 100.50, datetime.datetime(2023, 1, 15)),
            ("O002", "C002", 200.75, datetime.datetime(2023, 2, 20)),
            ("O003", "C003", 150.25, datetime.datetime(2023, 3, 25)),
            ("O004", None, 300.00, datetime.datetime(2023, 4, 30)),  # Null customer
            ("O001", "C001", 100.50, datetime.datetime(2023, 1, 15))  # Duplicate
        ]
        
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("CustId", StringType(), True),
            StructField("OrderAmount", DoubleType(), True),
            StructField("OrderDate", TimestampType(), True)
        ])
        
        cls.order_df = cls.spark.createDataFrame(order_data, schema=order_schema)
        
        # Create processor instance with test configuration
        cls.processor = OrderProcessor(cls.spark)
        
        # Override table names for testing
        cls.processor.customer_table = "test_customer"
        cls.processor.order_table = "test_order"
        cls.processor.order_summary_table = "test_order_summary"

    @classmethod
    def tearDownClass(cls):
        """Clean up resources after tests."""
        cls.spark.stop()

    def test_clean_data(self):
        """Test data cleaning functionality."""
        # Test customer data cleaning
        cleaned_customer_df = self.processor.clean_data(self.customer_df, ["CustId", "CustomerName"])
        self.assertEqual(cleaned_customer_df.count(), 3)  # Should remove null name and duplicate
        
        # Test order data cleaning
        cleaned_order_df = self.processor.clean_data(self.order_df, ["OrderId", "CustId"])
        self.assertEqual(cleaned_order_df.count(), 3)  # Should remove null CustId and duplicate

    def test_save_and_read_delta(self):
        """Test saving to and reading from Delta tables."""
        # Clean and save customer data
        cleaned_customer_df = self.processor.clean_data(self.customer_df, ["CustId", "CustomerName"])
        self.processor.save_to_delta(cleaned_customer_df, self.processor.customer_table)
        
        # Clean and save order data
        cleaned_order_df = self.processor.clean_data(self.order_df, ["OrderId", "CustId"])
        self.processor.save_to_delta(cleaned_order_df, self.processor.order_table)
        
        # Read back from Delta tables
        customer_from_delta = self.spark.table(self.processor.customer_table)
        order_from_delta = self.spark.table(self.processor.order_table)
        
        # Check counts
        self.assertEqual(customer_from_delta.count(), 3)
        self.assertEqual(order_from_delta.count(), 3)
        
        # Check schema includes processing_timestamp
        self.assertTrue("processing_timestamp" in customer_from_delta.columns)
        self.assertTrue("processing_timestamp" in order_from_delta.columns)

    def test_join_customer_order_data(self):
        """Test joining customer and order data."""
        # First ensure data is in Delta tables
        cleaned_customer_df = self.processor.clean_data(self.customer_df, ["CustId", "CustomerName"])
        self.processor.save_to_delta(cleaned_customer_df, self.processor.customer_table)
        
        cleaned_order_df = self.processor.clean_data(self.order_df, ["OrderId", "CustId"])
        self.processor.save_to_delta(cleaned_order_df, self.processor.order_table)
        
        # Test join
        joined_df = self.processor.join_customer_order_data()
        
        # Should have 3 rows (all valid customers with orders)
        self.assertEqual(joined_df.count(), 3)
        
        # Check join correctness - all orders should have matching customer info
        self.assertEqual(joined_df.filter(joined_df["order.CustId"] != joined_df["customer.CustId"]).count(), 0)

if __name__ == "__main__":
    unittest.main()