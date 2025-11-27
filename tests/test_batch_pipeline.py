import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from batch_pipeline import clean_data

class TestBatchPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Create a Spark session for testing."""
        cls.spark = SparkSession.builder \
            .appName("TestCustomerOrderProcessing") \
            .master("local[*]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session."""
        cls.spark.stop()
    
    def test_clean_data(self):
        """Test the clean_data function."""
        # Create sample customer data with nulls and duplicates
        customer_data = [
            (1, "John Doe", "john@example.com", "North"),
            (2, "Jane Smith", "jane@example.com", "South"),
            (3, None, "bob@example.com", "East"),
            (4, "Alice Brown", None, "West"),
            (1, "John Doe", "john@example.com", "North")  # Duplicate
        ]
        customer_schema = ["CustId", "Name", "EmailId", "Region"]
        customer_df = self.spark.createDataFrame(customer_data, customer_schema)
        
        # Create sample order data with nulls and duplicates
        order_data = [
            (101, "Item1", 10.0, 2, "2023-01-01", 1),
            (102, "Item2", 15.0, 1, "2023-01-02", 2),
            (103, None, 20.0, 3, "2023-01-03", 3),
            (104, "Item4", None, 2, "2023-01-04", 4),
            (101, "Item1", 10.0, 2, "2023-01-01", 1)  # Duplicate
        ]
        order_schema = ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
        order_df = self.spark.createDataFrame(order_data, order_schema)
        
        # Clean the data
        clean_customer_df, clean_order_df = clean_data(customer_df, order_df)
        
        # Check that nulls and duplicates are removed from customer data
        self.assertEqual(clean_customer_df.count(), 2)
        
        # Check that nulls and duplicates are removed from order data
        self.assertEqual(clean_order_df.count(), 2)
        
        # Check that TotalAmount is calculated correctly
        order_with_total = clean_order_df.filter(F.col("OrderId") == 101).first()
        self.assertEqual(order_with_total["TotalAmount"], 20.0)  # 10.0 * 2
        
        order_with_total = clean_order_df.filter(F.col("OrderId") == 102).first()
        self.assertEqual(order_with_total["TotalAmount"], 15.0)  # 15.0 * 1

if __name__ == "__main__":
    unittest.main()