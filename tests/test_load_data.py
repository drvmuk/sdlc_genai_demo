"""
Tests for load_data module
"""
import pytest
from unittest.mock import patch, MagicMock
from src.load_data import load_csv_to_delta

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession"""
    spark = MagicMock()
    return spark

@patch("src.load_data.SparkSession")
def test_load_csv_to_delta_success(mock_spark_session