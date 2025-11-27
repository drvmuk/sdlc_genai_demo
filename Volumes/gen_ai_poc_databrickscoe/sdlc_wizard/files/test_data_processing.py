import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import sys
import os

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_processing import DataProcessor

class TestDataProcessor(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create Spark session for tests
        cls.spark = SparkSession.builder \
            .appName("TestDataProcessor") \
            .master("local[1]") \
            .getOrCreate()
            
    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()
    
    def setUp(self):
        # Create test data
        self.customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C004", "Alice Brown", "alice@example.com", "West"),
            ("C005", "Null", "null@example.com", "North"),
            (None, "Invalid Customer", "invalid@example.com", "South"),
            ("C001", "John Doe", "john@example.com", "North")  # Duplicate
        ]
        
        self.order_data = [
            ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Phone", 800.0, 2, datetime.date(2023, 2, 20), "C002"),
            ("O003", "Tablet", 500.0, 1, datetime.date(2023, 3, 10), "C003"),
            ("O004", "Monitor", 300.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O005", "Keyboard", 100.0, 3, datetime.date(2023, 2, 25), "C004"),
            ("O006", "Mouse", 50.0, 2, datetime.date(2023, 2, 25), "C004"),
            (None, "Invalid Item", 0.0, 0, datetime.date(2023, 3, 15), "C005"),
            ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001")  # Duplicate
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
        
        self.customer_df = self.spark.createDataFrame(self.customer_data, schema=customer_schema)
        self.order_df = self.spark.createDataFrame(self.order_data, schema=order_schema)
        
        # Create processor with mocked spark session
        self.processor = DataProcessor(self.spark)
        
    def test_clean_data(self):
        # Test cleaning customer data
        clean_customer_df = self.processor.clean_data(self.customer_df)
        
        # Check row count (should remove nulls and duplicates)
        self.assertEqual(clean_customer_df.count(), 4)
        
        # Test cleaning order data
        clean_order_df = self.processor.clean_data(self.order_df)
        
        # Check row count (should remove nulls and duplicates)
        self.assertEqual(clean_order_df.count(), 6)
    
    @patch('src.data_processing.DataProcessor.read_source_data')
    def test_process_data(self, mock_read_source):
        # Mock read_source_data to return test data
        mock_read_source.return_value = (self.customer_df, self.order_df)
        
        # Mock table creation and writing methods
        self.processor.create_ordersummary_table = MagicMock()
        self.processor.create_customeraggregatespend_table = MagicMock()
        
        # Mock spark table operations
        self.spark.sql = MagicMock()
        
        # Run the process
        with patch.object(self.processor, 'load_scd_type2') as mock_load_scd:
            with patch.object(self.processor, 'aggregate_customer_spend') as mock_aggregate:
                self.processor.process_data()
                
                # Verify methods were called
                mock_read_source.assert_called_once()
                mock_load_scd.assert_called_once()
                mock_aggregate.assert_called_once()

if __name__ == '__main__':
    unittest.main()