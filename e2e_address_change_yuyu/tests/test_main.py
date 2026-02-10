"""
Tests for the main module.
"""
import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
import main


@patch("main.SparkSession")
@patch("main.AddressChangeProcessor")
def test_main_execution(mock_processor_class, mock_spark_builder):
    """Test the main function execution flow."""
    # Setup mocks
    mock_spark = MagicMock()
    mock_spark_builder.builder.appName.return_value.enableHiveSupport.return_value.getOrCreate.return_value = mock_spark
    
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor
    
    # Mock command line arguments
    test_args = [
        "--process-userid", "TEST_USER",
        "--source-table", "test_source",
        "--target-table", "test_target",
        "--output-path", "/tmp/test_output"
    ]
    
    with patch("sys.argv", ["main.py"] + test_args):
        # Execute main
        main.main()
        
        # Verify SparkSession creation
        mock_spark_builder.builder.appName.assert_called_with("E2E Address Change YUYU Processing")
        mock_spark_builder.builder.appName.return_value.enableHiveSupport.assert_called_once()
        
        # Verify processor creation and execution
        mock_processor_class.assert_called_with(mock_spark, "TEST_USER")
        mock_processor.process.assert_called_with("test_source", "test_target", "/tmp/test_output")
        
        # Verify SparkSession stop
        mock_spark.stop.assert_called_once()