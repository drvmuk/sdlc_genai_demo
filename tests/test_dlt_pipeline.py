"""
Unit tests for the DLT pipeline.
"""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import datetime
import os
import tempfile
import shutil

class TestDLTPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session and test data."""
        # Create Spark session
        cls.spark = SparkSession.builder \
            .appName("TestDLTPipeline") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create temporary directory for test data
        cls.test_dir = tempfile.mkdtemp()
        cls.customer_path = os.path.join(cls.test_dir, "customer")
        cls.order_path = os.path.join(cls.test_dir, "order")
        
        # Create test customer data
        customer_data = [
            (1, "John Doe", "123 Main St", "555-1234", "john@example.com"),
            (2, "Jane Smith", "456 Oak Ave", "555-5678", "jane@example.com"),
            (3, "Bob Johnson", "789 Pine Rd", "555-9012", "bob@example.com")
        ]
        customer_schema = ["CustId", "Name", "Address", "Phone", "Email"]
        cls.customer_df = cls.spark.createDataFrame(customer_data, schema=customer_schema)
        cls.customer_df.write.format("csv").option("header", "true").save(cls.customer_path)
        
        # Create test order data
        order_data = [
            (101, 1, "Product A", 10.0, 2, "2023-01-15"),
            (102, 2, "Product B", 15.0, 1, "2023-01-16"),
            (103, 3, "Product C", 20.0, 3, "2023-01-17"),
            (104, 1, "Product D", 25.0, 1, "2023-01-18")
        ]
        order_schema = ["OrderId", "CustId", "Product", "PricePerUnit", "Qty", "Date"]
        cls.order_df = cls.spark.createDataFrame(order_data, schema=order_schema)
        cls.order_df.write.format("csv").option("header", "true").save(cls.order_path)
        
        # Import functions to test - we need to mock the dlt module
        import sys
        import types
        
        # Create a mock dlt module
        mock_dlt = types.ModuleType("dlt")
        mock_dlt.table = lambda name, comment: lambda f: f
        mock_dlt.read = lambda table_name: cls.spark.createDataFrame([], "CustId STRING")
        mock_dlt.apply_changes = lambda target, source, keys, sequence_by, apply_as_deletes, column_list: lambda f: f
        mock_dlt.table_property = lambda f: f
        sys.modules["dlt"] = mock_dlt
        
        # Now we can import our module
        from src.utils import remove_nulls_and_duplicates
        cls.remove_nulls_and_duplicates = remove_nulls_and_duplicates
    
    @classmethod
    def tearDownClass(cls):
        """Clean up resources."""
        # Stop Spark session
        cls.spark.stop()
        
        # Remove temporary directory
        shutil.rmtree(cls.test_dir)
    
    def test_remove_nulls_and_duplicates(self):
        """Test the remove_nulls_and_duplicates function."""
        # Create a DataFrame with nulls and duplicates
        data = [
            (1, "John", None),
            (2, "Jane", "Active"),
            (2, "Jane", "Active"),  # Duplicate
            (3, None, "Active"),    # Null value
            (4, "Bob", "Inactive")
        ]
        schema = ["id", "name", "status"]
        df = self.spark.createDataFrame(data, schema=schema)
        
        # Apply the function
        result = self.remove_nulls_and_duplicates(df)
        
        # Check the result
        self.assertEqual(result.count(), 2)  # Should have 2 rows after removing nulls and duplicates
        
        # Test with key_columns parameter
        result_with_keys = self.remove_nulls_and_duplicates(df, key_columns=["id"])
        self.assertEqual(result_with_keys.count(), 3)  # Should have 3 rows after removing duplicates by id
    
    def test_calculate_total_amount(self):
        """Test the calculation of TotalAmount."""
        # Read the order data
        order_df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(self.order_path)
        
        # Calculate TotalAmount
        df_with_total = order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        
        # Check the results
        result = df_with_total.collect()
        self.assertEqual(result[0]["TotalAmount"], 20.0)  # 10.0 * 2
        self.assertEqual(result[1]["TotalAmount"], 15.0)  # 15.0 * 1
        self.assertEqual(result[2]["TotalAmount"], 60.0)  # 20.0 * 3
        self.assertEqual(result[3]["TotalAmount"], 25.0)  # 25.0 * 1

if __name__ == "__main__":
    unittest.main()