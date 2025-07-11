"""
Unit tests for utility functions.
"""
import unittest
from unittest.mock import patch, MagicMock
import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from src.utils import log_execution, retry_operation, add_audit_columns

class TestUtils(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for tests."""
        cls.spark = SparkSession.builder \
            .appName("TestUtils") \
            .master("local[*]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up resources."""
        cls.spark.stop()
    
    @patch('logging.Logger.info')
    @patch('logging.Logger.error')
    def test_log_execution_success(self, mock_error, mock_info):
        """Test log_execution decorator with successful function execution."""
        @log_execution
        def test_func():
            return "success"
        
        result = test_func()
        
        self.assertEqual(result, "success")
        mock_info.assert_any_call("Starting execution of test_func")
        mock_info.assert_any_call(unittest.mock.ANY)  # Check for completion log
        mock_error.assert_not_called()
    
    @patch('logging.Logger.info')
    @patch('logging.Logger.error')
    def test_log_execution_failure(self, mock_error, mock_info):
        """Test log_execution decorator with failing function."""
        @log_execution
        def test_func_fail():
            raise ValueError("Test error")
        
        with self.assertRaises(ValueError):
            test_func_fail()
        
        mock_info.assert_called_once_with("Starting execution of test_func_fail")
        mock_error.assert_called_once_with("Error in test_func_fail: Test error")
    
    @patch('time.sleep')
    def test_retry_operation_success(self, mock_sleep):
        """Test retry_operation decorator with successful function."""
        counter = {'value': 0}
        
        @retry_operation
        def test_func():
            counter['value'] += 1
            return "success"
        
        result = test_func()
        
        self.assertEqual(result, "success")
        self.assertEqual(counter['value'], 1)  # Function called once
        mock_sleep.assert_not_called()
    
    @patch('time.sleep')
    def test_retry_operation_failure_then_success(self, mock_sleep):
        """Test retry_operation decorator with function that fails then succeeds."""
        counter = {'value': 0}
        
        @retry_operation
        def test_func():
            counter['value'] += 1
            if counter['value'] < 2:
                raise ValueError("Test error")
            return "success"
        
        result = test_func()
        
        self.assertEqual(result, "success")
        self.assertEqual(counter['value'], 2)  # Function called twice
        mock_sleep.assert_called_once()
    
    def test_add_audit_columns(self):
        """Test add_audit_columns function."""
        # Create a simple DataFrame
        data = [(1, "John"), (2, "Jane")]
        schema = ["id", "name"]
        df = self.spark.createDataFrame(data, schema=schema)
        
        # Add audit columns
        result = add_audit_columns(df)
        
        # Check that audit columns were added
        self.assertTrue("created_at" in result.columns)
        self.assertTrue("updated_at" in result.columns)
        self.assertEqual(result.count(), 2)  # Row count should remain the same

if __name__ == "__main__":
    unittest.main()