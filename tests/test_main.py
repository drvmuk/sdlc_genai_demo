"""
Unit tests for the main module.
"""
import pytest
from unittest.mock import patch, MagicMock
from src.main import run_pipeline

def test_run_pipeline():
    """Test the full pipeline execution."""
    with patch('src.main.get_spark_session') as mock_get_spark, \
         patch('src.main.extract_source_data') as mock_extract, \
         patch('src.main.transform_policy_data') as mock_transform, \
         patch('src.main.load_to_target') as mock_load, \
         patch('src.main.logging') as mock_logging:
        
        # Configure mocks
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        
        mock_source_tables = MagicMock()
        mock_extract.return_value = mock_source_tables
        
        mock_transformed_data = MagicMock()
        mock_transform.return_value = mock_transformed_data
        
        # Call the function
        run_pipeline()
        
        # Verify the pipeline steps were executed in order
        mock_get_spark.assert_called_once()
        mock_extract.assert_called_once_with(mock_spark)
        mock_transform.assert_called_once_with(mock_source_tables)
        mock_load.assert_called_once_with(mock_spark, mock_transformed_data)
        mock_spark.stop.assert_called_once()

def test_run_pipeline_exception():
    """Test pipeline error handling."""
    with patch('src.main.get_spark_session') as mock_get_spark, \
         patch('src.main.extract_source_data') as mock_extract, \
         patch('src.main.logging') as mock_logging:
        
        # Configure mock to raise an exception
        mock_extract.side_effect = Exception("Test error")
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        
        # Call the function and expect it to raise the exception
        with pytest.raises(Exception, match="Test error"):
            run_pipeline()
        
        # Verify spark session was still stopped
        mock_spark.stop.assert_called_once()