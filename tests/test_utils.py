import unittest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import tempfile
import shutil

# Import the module to test
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.utils import validate_data, log_pipeline_metrics

class TestUtils(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession and test data"""
        cls.spark = SparkSession.builder \
            .appName("TestUtils") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
            .getOrCreate()
        
        # Create test data
        cls.good_data = cls.spark.createDataFrame([
            (1, "John Doe", "123 Main St"),
            (2, "Jane Smith", "456 Oak Ave"),
            (3, "Bob Johnson", "789 Pine Rd")
        ], ["id", "name", "address"])
        
        cls.data_with_nulls = cls.spark.createDataFrame([
            (1, "John Doe", "123 Main St"),
            (2, None, "456 Oak Ave"),
            (3, "Bob Johnson", None)
        ], ["id", "name", "address"])
        
        cls.data_with_duplicates = cls.spark.createDataFrame([
            (1, "John Doe", "123 Main St"),
            (2, "Jane Smith", "456 Oak Ave"),
            (2, "Jane Smith", "456 Oak Ave"),  # Duplicate
            (3, "Bob Johnson", "789 Pine Rd")
        ], ["id", "name", "address"])
    
    @classmethod
    def tearDownClass(cls):
        """Clean up resources"""
        cls.spark.stop()
    
    def test_validate_data_good(self):
        """Test data validation with good data"""
        is_valid, errors = validate_data(self.good_data, "good_data_table")
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
    
    def test_validate_data_with_nulls(self):
        """Test data validation with null values"""
        is_valid, errors = validate_data(self.data_with_nulls, "null_data_table")
        self.assertFalse(is_valid)
        self.assertEqual(len(errors), 1)
        self.assertTrue("Null values found" in errors[0])
    
    def test_validate_data_with_duplicates(self):
        """Test data validation with duplicate records"""
        is_valid, errors = validate_data(self.data_with_duplicates, "duplicate_data_table")
        self.assertFalse(is_valid)
        self.assertEqual(len(errors), 1)
        self.assertTrue("duplicate rows" in errors[0])
    
    def test_log_pipeline_metrics_success(self):
        """Test logging pipeline metrics for successful execution"""
        start_time = datetime.now()
        end_time = datetime.now()
        
        # This should not raise any exceptions
        log_pipeline_metrics("test_pipeline", start_time, end_time, 100, True)
    
    def test_log_pipeline_metrics_failure(self):
        """Test logging pipeline metrics for failed execution"""
        start_time = datetime.now()
        end_time = datetime.now()
        
        # This should not raise any exceptions
        log_pipeline_metrics("test_pipeline", start_time, end_time, 50, False, "Test error message")

if __name__ == "__main__":
    unittest.main()