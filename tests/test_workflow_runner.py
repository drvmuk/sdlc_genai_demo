"""
Unit tests for the workflow runner.
"""
import pytest
import os
import sys
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

# Add the src directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.workflow_runner import main, get_config, setup_logging


@pytest.fixture
def mock_spark_session():
    """Create a mock SparkSession."""
    mock_session = MagicMock()
    mock_builder = MagicMock()
    mock_builder.appName.return_value = mock_builder
    mock_builder.enableHiveSupport.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session
    
    with patch('pyspark.sql.SparkSession.builder', mock_builder):
        yield mock_session


def test_get_config():
    """Test the get_config function."""
    # Test with default values
    config = get_config()
    assert config["target_dir"] == "/dbfs/mnt/target/files"
    assert config["batch_size"] == 10000
    
    # Test with environment variables
    with patch.dict(os.environ, {"TARGET_FILE_DIR": "/custom/path", "BATCH_SIZE": "5000"}):
        config = get_config()
        assert config["target_dir"] == "/custom/path"
        assert config["batch_size"] == 5000


def test_setup_logging():
    """Test the setup_logging function."""
    with patch('logging.FileHandler') as mock_file_handler, \
         patch('logging.StreamHandler') as mock_stream_handler, \
         patch('logging.basicConfig') as mock_basic_config, \
         patch('os.makedirs') as mock_makedirs:
        
        logger = setup_logging()
        
        # Check that the log directory is created
        mock_makedirs.assert_called_once()
        
        # Check that logging is configured
        mock_basic_config.assert_called_once()
        
        # Check that the logger is returned
        assert logger is not None


@patch('src.workflow_runner.run_workflow')
def test_main_success(mock_run_workflow, mock_spark_session):
    """Test successful execution of the main function."""
    with patch('src.workflow_runner.setup_logging') as mock_setup_logging:
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger
        
        result = main()
        
        # Check that logging was set up
        mock_setup_logging.assert_called_once()
        
        # Check that the workflow was run
        mock_run_workflow.assert_called_once()
        
        # Check that success was logged
        mock_logger.info.assert_any_call("Workflow completed successfully")
        
        # Check the return code
        assert result == 0


@patch('src.workflow_runner.run_workflow')
def test_main_failure(mock_run_workflow, mock_spark_session):
    """Test handling of failures in the main function."""
    with patch('src.workflow_runner.setup_logging') as mock_setup_logging:
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger
        
        # Make the workflow raise an exception
        mock_run_workflow.side_effect = Exception("Test error")
        
        result = main()
        
        # Check that the error was logged
        mock_logger.error.assert_called_once()
        
        # Check the return code
        assert result == 1