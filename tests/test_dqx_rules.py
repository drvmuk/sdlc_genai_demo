"""
Tests for custom DQX rules implementation.
"""

import pytest
from pyspark.sql import SparkSession
import logging
from unittest.mock import MagicMock, patch
import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from custom_dqx_rules.dqx_rules import DQXCustomRules, dqx_null_check, dqx_primary_key_check

# Configure logging
logging.basicConfig(level=logging.INFO)

@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing.
    """
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("DQXCustomRulesTest")
        .getOrCreate()
    )

@pytest.fixture
def sample_df(spark):
    """
    Create a sample DataFrame for testing.
    """
    data = [
        (1, "John", "john@example.com", "2023-01-01"),
        (2, "Jane", None, "2023-01-02"),
        (3, "Bob", "bob@example.com", "2023-01-03"),
        (3, "Duplicate", "duplicate@example.com", "2023-01-04"),  # Duplicate ID
        (4, None, "alice@example.com", "2023-01-05")
    ]
    columns = ["id", "name", "email", "date"]
    return spark.createDataFrame(data, columns)

@patch('custom_dqx_rules.dqx_rules.DQEngine')
@patch('custom_dqx_rules.dqx_rules.WorkspaceClient')
def test_dqx_rules_initialization(mock_workspace_client, mock_dq_engine):
    """
    Test initialization of DQXCustomRules.
    """
    # Arrange
    mock_client = MagicMock()
    mock_workspace_client.return_value = mock_client
    
    # Act
    dqx_rules = DQXCustomRules()
    
    # Assert
    assert dqx_rules.workspace_client == mock_client
    assert dqx_rules.dq_engine is None

def test_null_check_rule():
    """
    Test the null check rule function.
    """
    # Act
    condition = dqx_null_check("test_column")
    
    # Assert
    assert "test_column" in str(condition)
    assert "isNull" in str(condition)

def test_primary_key_check_rule():
    """
    Test the primary key check rule function.
    """
    # Act
    condition = dqx_primary_key_check(["id", "code"], "test_table")
    
    # Assert
    assert "id" in str(condition)
    assert "code" in str(condition)
    assert "test_table" in str(condition)
    assert "count() over" in str(condition)

@patch('custom_dqx_rules.dqx_rules.DQEngine')
def test_apply_dq_checks(mock_dq_engine, sample_df):
    """
    Test applying DQ checks to a DataFrame.
    """
    # Arrange
    mock_engine_instance = MagicMock()
    mock_dq_engine.return_value = mock_engine_instance
    mock_engine_instance.apply_checks.return_value = sample_df
    
    dqx_rules = DQXCustomRules()
    rule_config = {
        "rules": {
            "null_check": {
                "for_each_column": ["name", "email"],
                "args": {
                    "column_name": "${column}"
                }
            }
        }
    }
    
    # Act
    result_df = dqx_rules.apply_dq_checks(sample_df, rule_config)
    
    # Assert
    mock_engine_instance.apply_checks.assert_called_once()
    assert result_df is not None
    assert result_df is sample_df  # Since we're mocking the return value

def test_default_rule_config():
    """
    Test the default rule configuration.
    """
    # Arrange
    dqx_rules = DQXCustomRules()
    
    # Act
    config = dqx_rules._get_default_rule_config()
    
    # Assert
    assert "rules" in config
    assert "null_check" in config["rules"]
    assert "primary_key_check" in config["rules"]
    assert "for_each_column" in config["rules"]["null_check"]
    assert config["rules"]["null_check"]["for_each_column"] == ["*"]