"""
Tests for utility functions
"""
import pytest
from unittest.mock import patch, MagicMock
from src.utils import table_exists

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession"""
    spark = MagicMock()
    return spark

def test_table_exists_three_part_name_exists(mock_spark):
    """Test table_exists with three-part name when table exists"""
    # Setup
    mock_row = MagicMock()
    mock_row.tableName = "customer"
    mock_spark.sql.return_value.collect.return_value = [mock_row]
    
    # Execute
    result = table_exists(mock_spark, "gen_ai_poc_databrickscoe.sdlc_wizard.customer")
    
    # Verify
    assert result is True
    mock_spark.sql.assert_called_once_with("SHOW TABLES IN gen_ai_poc_databrickscoe.sdlc_wizard")

def test_table_exists_three_part_name_not_exists(mock_spark):
    """Test table_exists with three-part name when table does not exist"""
    # Setup
    mock_row = MagicMock()
    mock_row.tableName = "other_table"
    mock_spark.sql.return_value.collect.return_value = [mock_row]
    
    # Execute
    result = table_exists(mock_spark, "gen_ai_poc_databrickscoe.sdlc_wizard.customer")
    
    # Verify
    assert result is False
    mock_spark.sql.assert_called_once_with("SHOW TABLES IN gen_ai_poc_databrickscoe.sdlc_wizard")

def test_table_exists_two_part_name(mock_spark):
    """Test table_exists with two-part name"""
    # Setup
    mock_row = MagicMock()
    mock_row.tableName = "customer"
    mock_spark.sql.return_value.collect.return_value = [mock_row]
    
    # Execute
    result = table_exists(mock_spark, "sdlc_wizard.customer")
    
    # Verify
    assert result is True
    mock_spark.sql.assert_called_once_with("SHOW TABLES IN sdlc_wizard")

def test_table_exists_exception(mock_spark):
    """Test table_exists when an exception occurs"""
    # Setup
    mock_spark.sql.side_effect = Exception("Test exception")
    
    # Execute
    with patch("src.utils.logging.error") as mock_log_error:
        result = table_exists(mock_spark, "gen_ai_poc_databrickscoe.sdlc_wizard.customer")
    
    # Verify
    assert result is False
    mock_log_error.assert_called_once()