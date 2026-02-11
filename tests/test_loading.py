"""
Unit tests for the loading module.
"""
import pytest
from unittest.mock import patch, MagicMock
from src.loading import load_to_target

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession for testing."""
    return MagicMock()

@pytest.fixture
def mock_dataframe():
    """Create a mock DataFrame for testing."""
    df = MagicMock()
    writer = MagicMock()
    df.write = writer
    writer.format.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer
    df.count.return_value = 100
    return df

def test_load_to_target(mock_spark, mock_dataframe):
    """Test loading data to the target table."""
    with patch('src.loading.DB_CONFIG', {
        "target": {
            "jdbc_url": "jdbc:test",
            "user": "user",
            "password": "password",
            "driver": "test.driver"
        }
    }), patch('src.loading.TARGET_TABLE', "TARGET_TABLE"), \
         patch('src.loading.BATCH_SIZE', 1000):
        
        load_to_target(mock_spark, mock_dataframe)
        
        mock_dataframe.write.format.assert_called_once_with("jdbc")
        mock_dataframe.write.format().option.assert_any_call("url", "jdbc:test")
        mock_dataframe.write.format().option.assert_any_call("dbtable", "TARGET_TABLE")
        mock_dataframe.write.format().option.assert_any_call("user", "user")
        mock_dataframe.write.format().option.assert_any_call("password", "password")
        mock_dataframe.write.format().option.assert_any_call("driver", "test.driver")
        mock_dataframe.write.format().option.assert_any_call("batchsize", 1000)
        mock_dataframe.write.format().option().option().option().option().option().option().mode.assert_called_once_with("append")
        mock_dataframe.write.format().option().option().option().option().option().option().mode().save.assert_called_once()