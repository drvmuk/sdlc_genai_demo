"""
Unit tests for the transformation module.
"""
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from src.transformation import transform_policy_data

@pytest.fixture
def mock_source_tables():
    """Create mock source tables for testing."""
    tables = {}
    for table_name in ["T_TX_REQUEST_POLICY", "T_TX_BASIC", "T_TX_RELATION", 
                       "T_TX_REQUEST", "T_TX_DTL_BENEFICIARY_CHANGE"]:
        df = MagicMock(spec=DataFrame)
        df.alias.return_value = df
        df.join.return_value = df
        df.select.return_value = df
        df.filter.return_value = df
        tables[table_name] = df
    return tables

def test_transform_policy_data(mock_source_tables):
    """Test the transformation of policy data."""
    with patch('src.transformation.DEFAULT_VALUES', {
        "DATA_TYPE": "TEST_TYPE",
        "POLICY_TYPE": "TEST_POLICY",
        "MC_CRNCY": "USD"
    }), patch('src.transformation.col') as mock_col, \
         patch('src.transformation.lit') as mock_lit, \
         patch('src.transformation.current_timestamp') as mock_timestamp, \
         patch('src.transformation.monotonically_increasing_id') as mock_id:
        
        # Configure mocks
        mock_col.return_value = MagicMock()
        mock_lit.return_value = MagicMock()
        mock_timestamp.return_value = MagicMock()
        mock_id.return_value = MagicMock()
        
        # Call the function
        result = transform_policy_data(mock_source_tables)
        
        # Verify the joins were made
        trp = mock_source_tables["T_TX_REQUEST_POLICY"]
        ttb = mock_source_tables["T_TX_BASIC"]
        tr = mock_source_tables["T_TX_REQUEST"]
        ttr = mock_source_tables["T_TX_RELATION"]
        ttdbc = mock_source_tables["T_TX_DTL_BENEFICIARY_CHANGE"]
        
        trp.alias.assert_called_once_with("trp")
        ttb.alias.assert_called_once_with("ttb")
        tr.alias.assert_called_once_with("tr")
        ttr.alias.assert_called_once_with("ttr")
        ttdbc.alias.assert_called_once_with("ttdbc")
        
        # Verify joins and transformations
        assert trp.join.called
        assert result.filter.called
        
        # The final result should be the filtered DataFrame
        assert result == mock_source_tables["T_TX_REQUEST_POLICY"].alias().join().join().join().join().select().filter()