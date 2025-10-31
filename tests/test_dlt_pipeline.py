import unittest
from pyspark.sql import SparkSession
import tempfile
import os
import shutil
from datetime import date
import pandas as pd
import sys
import json

# Add the src directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class TestDLTPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a local Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("DLTPipelineTest") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create temp directories for test data
        cls.test_dir = tempfile.mkdtemp()
        cls.customer_data_path = os.path.join(cls.test_dir, "customer_data")
        cls.order_data_path = os.path.join(cls.test_dir, "order_data")
        
        # Create test data
        cls._create_test_data()
        
        # Mock the dlt module
        sys.modules['dlt'] = type('obj', (object,), {
            'table': lambda **kwargs: lambda f: f,
            'read': lambda table_name: cls._mock_dlt_read(table_name),
            'expect_all_or_drop': lambda expectations: lambda f: f
        })
        
        # Import the module after mocking
        from src.dlt_pipeline import customer, order, ordersummary, customeraggregatespend
        cls.customer_func = customer
        cls.order_func = order
        cls.ordersummary_func = ordersummary
        cls.customeraggregatespend_func = customeraggregatespend

    @classmethod
    def tearDownClass(cls):
        # Clean up
        cls.spark.stop()
        shutil.rmtree(cls.test_dir)
    
    @classmethod
    def _create_test_data(cls):
        # Create customer test data
        customer_data = [
            {"CustId": "C001", "Name": "John Doe", "EmailId": "john@example.com", "Region": "North"},
            {"CustId": "C002", "Name": "Jane Smith", "EmailId": "jane@example.com", "Region": "South"},
            {"CustId": "C003", "Name": "Bob Johnson", "EmailId": "bob@example.com", "Region": "East"},
            {"CustId": "C004", "Name": "Alice Brown", "EmailId": "alice@example.com", "Region": "West"},
            {"CustId": "C005", "Name": "Charlie Wilson", "EmailId": "charlie@example.com", "Region": "North"},
            # Duplicate record
            {"CustId": "C001", "Name": "John Doe", "EmailId": "john@example.com", "Region": "North"},
            # Record with null
            {"CustId": "C006", "Name": None, "EmailId": "mark@example.com", "Region": "South"}
        ]
        
        # Create order test data
        order_data = [
            {"OrderId": "O001", "ItemName": "Laptop", "PricePerUnit": 1000.0, "Qty": 2, "Date": "2023-01-15", "CustId": "C001"},
            {"OrderId": "O002", "ItemName": "Phone", "PricePerUnit": 500.0, "Qty": 1, "Date": "2023-01-20", "CustId": "C002"},
            {"OrderId": "O003", "ItemName": "Tablet", "PricePerUnit": 300.0, "Qty": 3, "Date": "2023-01-25", "CustId": "C003"},
            {"OrderId": "O004", "ItemName": "Monitor", "PricePerUnit": 200.0, "Qty": 2, "Date": "2023-02-01", "CustId": "C001"},
            {"OrderId": "O005", "ItemName": "Keyboard", "PricePerUnit": 50.0, "Qty": 5, "Date": "2023-02-05", "CustId": "C002"},
            # Duplicate record
            {"OrderId": "O001", "ItemName": "Laptop", "PricePerUnit": 1000.0, "Qty": 2, "Date": "2023-01-15", "CustId": "C001"},
            # Record with null
            {"OrderId": "O006", "ItemName": "Mouse", "PricePerUnit": None, "Qty": 3, "Date": "2023-02-10", "CustId": "C004"}
        ]
        
        # Save as CSV files
        os.makedirs(cls.customer_data_path, exist_ok=True)
        os.makedirs(cls.order_data_path, exist_ok=True)
        
        customer_df = pd.DataFrame(customer_data)
        order_df = pd.DataFrame(order_data)
        
        customer_df.to_csv(os.path.join(cls.customer_data_path, "customer.csv"), index=False)
        order_df.to_csv(os.path.join(cls.order_data_path, "order.csv"), index=False)
    
    @classmethod
    def _mock_dlt_read(cls, table_name):
        if table_name == "customer":
            return cls.customer_func()
        elif table_name == "order":
            return cls.order_func()
        elif table_name == "ordersummary":
            # Create a mock ordersummary table for testing
            customer_df = cls.customer_func()
            order_df = cls.order_func()
            
            joined_df = customer_df.join(
                order_df,
                on="CustId",
                how="inner"
            ).select(
                customer_df["CustId"],
                customer_df["Name"],
                customer_df["EmailId"],
                customer_df["Region"],
                order_df["OrderId"],
                order_df["ItemName"],
                order_df["PricePerUnit"],
                order_df["Qty"],
                order_df["Date"],
                order_df["TotalAmount"]
            )
            
            from pyspark.sql import functions as F
            current_timestamp = F.current_timestamp()
            joined_df = joined_df.withColumn("IsActive", F.lit(True))
            joined_df = joined_df.withColumn("StartDate", current_timestamp)
            joined_df = joined_df.withColumn("EndDate", F.lit(None).cast("timestamp"))
            
            return joined_df
    
    def test_customer_table(self):
        # Override the path for testing
        import src.dlt_pipeline
        original_path = src.dlt_pipeline.CUSTOMER_DATA_PATH
        src.dlt_pipeline.CUSTOMER_DATA_PATH = self.customer_data_path
        
        try:
            # Run the function
            result = self.customer_func()
            
            # Verify results
            self.assertEqual(result.count(), 5)  # After removing duplicates and nulls
            
            # Check that all required columns exist
            columns = result.columns
            self.assertIn("CustId", columns)
            self.assertIn("Name", columns)
            self.assertIn("EmailId", columns)
            self.assertIn("Region", columns)
            
            # Verify no nulls
            self.assertEqual(result.filter("Name is null").count(), 0)
            
            # Verify no duplicates
            self.assertEqual(result.count(), result.dropDuplicates().count())
            
        finally:
            # Restore original path
            src.dlt_pipeline.CUSTOMER_DATA_PATH = original_path
    
    def test_order_table(self):
        # Override the path for testing
        import src.dlt_pipeline
        original_path = src.dlt_pipeline.ORDER_DATA_PATH
        src.dlt_pipeline.ORDER_DATA_PATH = self.order_data_path
        
        try:
            # Run the function
            result = self.order_func()
            
            # Verify results
            self.assertEqual(result.count(), 5)  # After removing duplicates and nulls
            
            # Check that all required columns exist
            columns = result.columns
            self.assertIn("OrderId", columns)
            self.assertIn("ItemName", columns)
            self.assertIn("PricePerUnit", columns)
            self.assertIn("Qty", columns)
            self.assertIn("Date", columns)
            self.assertIn("CustId", columns)
            self.assertIn("TotalAmount", columns)
            
            # Verify TotalAmount calculation
            laptop_order = result.filter("OrderId = 'O001'").first()
            self.assertEqual(laptop_order["TotalAmount"], 2000.0)  # 1000 * 2
            
            # Verify no nulls
            self.assertEqual(result.filter("PricePerUnit is null").count(), 0)
            
            # Verify no duplicates
            self.assertEqual(result.count(), result.dropDuplicates().count())
            
        finally:
            # Restore original path
            src.dlt_pipeline.ORDER_DATA_PATH = original_path
    
    def test_customeraggregatespend(self):
        # Run the function
        result = self.customeraggregatespend_func()
        
        # Verify results
        # Check that all required columns exist
        columns = result.columns
        self.assertIn("Name", columns)
        self.assertIn("Date", columns)
        self.assertIn("TotalAmount", columns)
        
        # Check aggregation logic
        # John Doe has two orders: O001 (2000) and O004 (400)
        john_spend = result.filter("Name = 'John Doe'").collect()
        
        # Convert to dictionary for easier testing
        john_spend_dict = {row["Date"].strftime("%Y-%m-%d"): row["TotalAmount"] for row in john_spend}
        
        # Check if John's total spend for Jan 15 is 2000
        self.assertAlmostEqual(john_spend_dict.get("2023-01-15", 0), 2000.0, places=1)
        
        # Check if John's total spend for Feb 1 is 400
        self.assertAlmostEqual(john_spend_dict.get("2023-02-01", 0), 400.0, places=1)

if __name__ == '__main__':
    unittest.main()