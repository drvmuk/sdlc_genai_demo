import unittest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import tempfile
import shutil
from datetime import datetime

# Import the module to test
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.dlt_pipeline import customer, order, ordersummary, ordersummary_scd2, customeraggregatespend

class TestDLTPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession and test data"""
        cls.spark = SparkSession.builder \
            .appName("TestDLTPipeline") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create test directories
        cls.test_dir = tempfile.mkdtemp()
        cls.customer_dir = os.path.join(cls.test_dir, "customer")
        cls.order_dir = os.path.join(cls.test_dir, "order")
        os.makedirs(cls.customer_dir)
        os.makedirs(cls.order_dir)
        
        # Create test data
        customer_data = [
            (1, "John Doe", "123 Main St"),
            (2, "Jane Smith", "456 Oak Ave"),
            (3, "Bob Johnson", "789 Pine Rd")
        ]
        
        order_data = [
            (1, 1, "2023-01-15", 100.50),
            (2, 2, "2023-01-16", 200.75),
            (3, 3, "2023-01-17", 150.25),
            (4, 1, "2023-01-18", 75.00)
        ]
        
        # Create DataFrames
        cls.customer_df = cls.spark.createDataFrame(customer_data, ["CustId", "Name", "Address"])
        cls.order_df = cls.spark.createDataFrame(order_data, ["OrderId", "CustId", "Date", "TotalAmount"])
        
        # Write test data to CSV
        cls.customer_df.write.csv(cls.customer_dir, header=True, mode="overwrite")
        cls.order_df.write.csv(cls.order_dir, header=True, mode="overwrite")
        
        # Mock DLT functions
        global dlt
        class MockDLT:
            @staticmethod
            def table(*args, **kwargs):
                def decorator(f):
                    return f
                return decorator
                
            @staticmethod
            def read(table_name):
                if table_name == "customer":
                    return cls.customer_df
                elif table_name == "order":
                    return cls.order_df
                elif table_name == "ordersummary":
                    # Create a mock ordersummary DataFrame
                    return cls.customer_df.join(
                        cls.order_df,
                        cls.customer_df.CustId == cls.order_df.CustId,
                        "inner"
                    ).withColumn("StartDate", F.lit(datetime.now())) \
                     .withColumn("EndDate", F.lit(None).cast("timestamp")) \
                     .withColumn("IsActive", F.lit(True))
                elif table_name == "ordersummary_scd2":
                    # Same as ordersummary for testing
                    return cls.customer_df.join(
                        cls.order_df,
                        cls.customer_df.CustId == cls.order_df.CustId,
                        "inner"
                    ).withColumn("StartDate", F.lit(datetime.now())) \
                     .withColumn("EndDate", F.lit(None).cast("timestamp")) \
                     .withColumn("IsActive", F.lit(True))
                return None
                
        dlt = MockDLT()
        
        # Set global variables
        global CUSTOMER_CSV_PATH, ORDER_CSV_PATH
        CUSTOMER_CSV_PATH = cls.customer_dir
        ORDER_CSV_PATH = cls.order_dir
        
        # Mock spark variable
        global spark
        spark = cls.spark
    
    @classmethod
    def tearDownClass(cls):
        """Clean up resources"""
        cls.spark.stop()
        shutil.rmtree(cls.test_dir)
    
    def test_customer_table(self):
        """Test customer table creation"""
        result_df = customer()
        
        # Check if the DataFrame is not empty
        self.assertGreater(result_df.count(), 0)
        
        # Check if all required columns exist
        expected_columns = ["CustId", "Name", "Address"]
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        
        # Check if the data is correct
        self.assertEqual(result_df.count(), 3)
    
    def test_order_table(self):
        """Test order table creation"""
        result_df = order()
        
        # Check if the DataFrame is not empty
        self.assertGreater(result_df.count(), 0)
        
        # Check if all required columns exist
        expected_columns = ["OrderId", "CustId", "Date", "TotalAmount"]
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        
        # Check if the data is correct
        self.assertEqual(result_df.count(), 4)
    
    def test_ordersummary_table(self):
        """Test ordersummary table creation"""
        result_df = ordersummary()
        
        # Check if the DataFrame is not empty
        self.assertGreater(result_df.count(), 0)
        
        # Check if all required columns exist
        expected_columns = ["CustId", "Name", "Address", "OrderId", "Date", "TotalAmount", "StartDate", "EndDate", "IsActive"]
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        
        # Check if the data is correct
        self.assertEqual(result_df.count(), 4)
        
        # Check if all records are active
        self.assertEqual(result_df.filter(F.col("IsActive") == True).count(), 4)
    
    def test_customeraggregatespend_table(self):
        """Test customeraggregatespend table creation"""
        result_df = customeraggregatespend()
        
        # Check if the DataFrame is not empty
        self.assertGreater(result_df.count(), 0)
        
        # Check if all required columns exist
        expected_columns = ["Name", "Date", "TotalSpend", "LastUpdated"]
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        
        # Check if the aggregation is correct
        john_spend = result_df.filter(F.col("Name") == "John Doe").select("TotalSpend").collect()[0][0]
        self.assertEqual(john_spend, 175.5)  # 100.50 + 75.00

if __name__ == "__main__":
    unittest.main()