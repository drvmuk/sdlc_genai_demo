"""
Unit tests for the extraction module.
"""
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.extraction import get_spark_session, read_source_table, extract_source_data

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession for testing."""
    spark = MagicMock()
    reader = MagicMock()
    spark.read = reader
    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = MagicMock()
    return spark

def test_get_spark_session():
    """Test that get_spark_session returns a SparkSession."""
    with patch.object(SparkSession, 'builder') as mock_builder:
        mock_session = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session
        
        session = get_spark_session()
        
        assert session == mock_session
        mock_builder.appName.assert_called_once_with("E2E_BC_K2H_DATA_EXTRACTION")
        assert mock_builder.config.call_count >= 2
        mock_builder.getOrCreate.assert_called_once()

def test_read_source_table(mock_spark):
    """Test reading a source table."""
    with patch('src.extraction.DB_CONFIG', {
        "source": {
            "jdbc_url": "jdbc:test",
            "user": "user",
            "password": "password",
            "driver": "test.driver"
        }
    }), patch('src.extraction.SOURCE_TABLES', {
        "TEST_TABLE": "schema.TEST_TABLE"
    }):
        df = read_source_table(mock_spark, "TEST_TABLE")
        
        mock_spark.read.format.assert_called_once_with("jdbc")
        mock_spark.read.format().option.assert_any_call("url", "jdbc:test")
        mock_spark.read.format().option.assert_any_call("dbtable", "schema.TEST_TABLE")
        mock_spark.read.format().option.assert_any_call("user", "user")
        mock_spark.read.format().option.assert_any_call("password", "password")
        mock_spark.read.format().option.assert_any_call("driver", "test.driver")
        mock_spark.read.format().option().option().option().option().option().load.assert_called_once()

def test_extract_source_data(mock_spark):
    """Test extracting all source tables."""
    with patch('src.extraction.SOURCE_TABLES', {
        "TABLE1": "schema.TABLE1",
        "TABLE2": "schema.TABLE2"
    }), patch('src.extraction.read_source_table') as mock_read:
        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_read.side_effect = [mock_df1, mock_df2]
        
        result = extract_source_data(mock_spark)
        
        assert mock_read.call_count == 2
        assert result == {"TABLE1": mock_df1, "TABLE2": mock_df2}