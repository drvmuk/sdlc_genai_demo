"""
Unit tests for the Delta Pipeline
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from src.delta_pipeline import DeltaPipeline

class TestDeltaPipeline(unittest.TestCase):
    """Test cases for the DeltaPipeline class"""
    
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession for all test cases"""
        cls.spark = SparkSession.builder \
            .appName("TestDeltaPipeline") \
            .master("local[2]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession after all tests"""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test case"""
        # Create sample customer data
        customer_data = [
            (1, "John Doe", "john@example.com", "North"),
            (2, "Jane Smith", "jane@example.com", "South"),
            (3, "Bob Johnson", "bob@example.com", "East"),
            (4, "Alice Brown", None, "West"),
            (5, None, "unknown@example.com", "North"),
            (1, "John Doe", "john@example.com", "North")  # Duplicate
        ]
        
        customer_schema = StructType([
            StructField("CustId", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        self.customer_df = self.spark.createDataFrame(customer_data, schema=customer_schema)
        
        # Create sample order data
        order_data = [
            (101, "Laptop", 1200.0, 1, "2023-01-15", 1),
            (102, "Mouse", 25.0, 2, "2023-01-20", 2),
            (103, "Keyboard", 50.0, 1, "2023-02-05", 3),
            (104, "Monitor", 200.0, 1, "2023-02-10", 1),
            (105, "Headphones", 100.0, 1, "2023-03-01", 4),
            (None, "Unknown", 10.0, 1, "2023-03-05", 5),
            (106, None, 30.0, 2, "2023-03-10", 2),
            (101, "Laptop", 1200.0, 1, "2023-01-15", 1)  # Duplicate
        ]
        
        order_schema = StructType([
            StructField("OrderId", IntegerType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("CustId", IntegerType(), True)
        ])
        
        self.order_df = self.spark.createDataFrame(order_data, schema=order_schema)
        
        # Create pipeline instance with mocked SparkSession
        self.pipeline = DeltaPipeline(self.spark)
    
    def test_clean_customer_data(self):
        """Test customer data cleaning logic"""
        # Run the cleaning function
        cleaned_df = self.pipeline.clean_customer_data(self.customer_df)
        
        # Convert to pandas for easier assertions
        cleaned_data = cleaned_df.select("CustId", "Name", "EmailId", "Region").toPandas()
        
        # Check that nulls in key fields are removed
        self.assertEqual(cleaned_data["CustId"].isna().sum(), 0)
        self.assertEqual(cleaned_data["Name"].isna().sum(), 0)
        
        # Check that duplicates are removed
        self.assertEqual(len(cleaned_data), 3)  # Should have 3 valid unique customers
        
        # Check that processed timestamp is added
        self.assertTrue("ProcessedTimestamp" in cleaned_df.columns)
    
    def test_clean_order_data(self):
        """Test order data cleaning logic"""
        # Run the cleaning function
        cleaned_df = self.pipeline.clean_order_data(self.order_df)
        
        # Convert to pandas for easier assertions
        cleaned_data = cleaned_df.select("OrderId", "ItemName", "PricePerUnit", "Qty", "CustId").toPandas()
        
        # Check that nulls in key fields are removed
        self.assertEqual(cleaned_data["OrderId"].isna().sum(), 0)
        self.assertEqual(cleaned_data["ItemName"].isna().sum(), 0)
        self.assertEqual(cleaned_data["CustId"].isna().sum(), 0)
        
        # Check that duplicates are removed
        self.assertEqual(len(cleaned_data), 4)  # Should have 4 valid unique orders
        
        # Check that TotalAmount is calculated correctly
        total_amounts = cleaned_df.select("PricePerUnit", "Qty", "TotalAmount").toPandas()
        for _, row in total_amounts.iterrows():
            self.assertEqual(row["TotalAmount"], row["PricePerUnit"] * row["Qty"])
        
        # Check that processed timestamp is added
        self.assertTrue("ProcessedTimestamp" in cleaned_df.columns)
    
    @patch('src.delta_pipeline.DeltaPipeline.save_to_delta')
    def test_run_pipeline(self, mock_save_to_delta):
        """Test the full pipeline execution"""
        # Mock the read_source_data method
        with patch.object(self.pipeline, 'read_source_data', return_value=(self.customer_df, self.order_df)):
            # Mock the create_summary_scd2 method
            with patch.object(self.pipeline, 'create_summary_scd2') as mock_create_summary:
                # Run the pipeline
                self.pipeline.run_pipeline()
                
                # Check that save_to_delta was called twice (for customer and order tables)
                self.assertEqual(mock_save_to_delta.call_count, 2)
                
                # Check that create_summary_scd2 was called once
                mock_create_summary.assert_called_once()


if __name__ == '__main__':
    unittest.main()