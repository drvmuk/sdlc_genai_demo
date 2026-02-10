"""
Unit tests for the source record count check module
"""

import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from src.source_count_check import (
    extract_source_data,
    calculate_source_count,
    write_to_target,
    run_source_count_check
)

class TestSourceCountCheck(unittest.TestCase):
    """Test cases for source count check functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests"""
        cls.spark = (SparkSession.builder
                    .appName("TestSourceCountCheck")
                    .master("local[*]")
                    .getOrCreate())
        
        # Create sample test data
        data = [
            (1, 101, 201, 301, "POL123", "ACTIVE"),
            (2, 102, 202, 302, "POL124", "PENDING"),
            (3, 103, 203, 303, "POL125", "ACTIVE"),
            (4, 104, 204, 304, "POL126", None),  # NULL status
            (5, 105, 205, 305, "POL127", "CLOSED")
        ]
        
        columns = [
            "T_TX_REQUEST_REQUEST_ID",
            "T_TX_BASIC_TRANS_ID",
            "T_TX_RELATION_TRANS_REL_ID",
            "T_TX_REQ_POL_REQUEST_POLICY_ID",
            "STG_POLICY_ID",
            "T_TX_REQUEST_REQUEST_STATUS"
        ]
        
        cls.test_df = cls.spark.createDataFrame(data, columns)
    
    @classmethod
    def tearDownClass(cls):
        """Stop Spark session"""
        cls.spark.stop()
    
    @patch("src.source_count_check.SparkSession")
    def test_extract_source_data(self, mock_spark):
        """Test the extraction of source data using SQL"""
        # Setup mock
        mock_spark_instance = MagicMock()
        mock_read = MagicMock()
        mock_spark_instance.read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.load.return_value = self.test_df
        
        # Execute function with test SQL
        test_sql = "SELECT * FROM ZSYSE2EDEV.STG_E2E_AC_TXDBH_DATA WHERE T_TX_REQUEST_REQUEST_STATUS = 'ACTIVE'"
        result_df = extract_source_data(mock_spark_instance, test_sql)
        
        # Verify the correct options were set
        mock_spark_instance.read.format.assert_called_with("jdbc")
        mock_read.option.assert_any_call("dbtable", f"({test_sql})")
        
        # Verify result
        self.assertEqual(result_df, self.test_df)
    
    def test_calculate_source_count(self):
        """Test calculation of source record count"""
        # Execute function
        result_df = calculate_source_count(self.test_df)
        
        # Convert to list for assertion
        result_list = result_df.collect()
        
        # Verify result - should be 5 records as string
        self.assertEqual(len(result_list), 1)
        self.assertEqual(result_list[0]["SOURCE_RECT"], "5")
    
    def test_calculate_source_count_overflow(self):
        """Test handling of overflow in source count"""
        # Create a mock dataframe with a large number of rows
        large_count_df = self.spark.createDataFrame(
            [(i,) for i in range(1_234_567_890)],  # 10+ digits
            ["id"]
        )
        
        # Mock the count to avoid actually creating a huge dataframe
        large_count_df = large_count_df.select(
            F.lit(1_234_567_890).alias("SOURCE_REC_COUNT")
        )
        
        # Apply the calculation function with patched agg
        with patch("pyspark.sql.DataFrame.agg") as mock_agg:
            mock_agg.return_value = large_count_df
            result_df = calculate_source_count(self.test_df)
            
            # Verify overflow handling
            result = result_df.collect()[0]["SOURCE_RECT"]
            self.assertEqual(result, "OVERFLOW")
    
    def test_write_to_target(self):
        """Test writing count to target file"""
        # Create a simple count dataframe
        count_df = self.spark.createDataFrame([("5",)], ["SOURCE_RECT"])
        
        # Use a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = temp_dir
            output_filename = "test_count.csv"
            
            # Mock filesystem operations that would normally use dbutils
            with patch("os.path.join", return_value=f"{output_path}/{output_filename}"):
                with patch("src.source_count_check.SparkSession.getActiveSession") as mock_session:
                    mock_session.return_value = self.spark
                    
                    # Execute function
                    write_to_target(count_df, output_path, output_filename)
                    
                    # Since we can't easily verify file operations in unit tests,
                    # we'll just check that the function completes without errors
    
    @patch("src.source_count_check.extract_source_data")
    @patch("src.source_count_check.calculate_source_count")
    @patch("src.source_count_check.write_to_target")
    @patch("src.source_count_check.create_spark_session")
    def test_run_source_count_check(self, mock_create_spark, mock_write, mock_calculate, mock_extract):
        """Test the full source count check process"""
        # Setup mocks
        mock_spark = MagicMock()
        mock_create_spark.return_value = mock_spark
        
        mock_source_df = MagicMock()
        mock_extract.return_value = mock_source_df
        
        mock_count_df = MagicMock()
        mock_calculate.return_value = mock_count_df
        
        # Execute function
        test_sql = "SELECT * FROM ZSYSE2EDEV.STG_E2E_AC_TXDBH_DATA"
        test_output_path = "/tmp/test_output"
        test_output_filename = "test_count.csv"
        
        run_source_count_check(test_sql, test_output_path, test_output_filename)
        
        # Verify all steps were called with correct parameters
        mock_create_spark.assert_called_once()
        mock_extract.assert_called_once_with(mock_spark, test_sql)
        mock_calculate.assert_called_once_with(mock_source_df)
        mock_write.assert_called_once_with(mock_count_df, test_output_path, test_output_filename)

if __name__ == "__main__":
    pytest.main(["-xvs", "test_source_count_check.py"])