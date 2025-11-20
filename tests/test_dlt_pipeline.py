import pytest
import tempfile
import os
import shutil
from pyspark.sql import SparkSession
from unittest.mock import patch, MagicMock
import sys

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Mock the dlt module since it's only available in Databricks
class MockDLT:
    @staticmethod
    def table(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    @staticmethod
    def expect_or_drop(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    @staticmethod
    def read(table_name):
        # This would normally return a DataFrame from a DLT table
        # For testing, we'll return a mock DataFrame
        if table_name == "customer":
            return mock_customer_df
        elif table_name == "order":
            return mock_order_df
        elif table_name == "ordersummary":
            return mock_ordersummary_df
        return None

# Create mock DataFrames
mock_customer_df = MagicMock()
mock_order_df = MagicMock()
mock_ordersummary_df = MagicMock()

# Mock the spark session
mock_spark = MagicMock()

@pytest.fixture(scope="module")
def setup_mocks():
    # Set up the mock DataFrames
    mock_customer_df.join.return_value = mock_customer_df
    mock_customer_df.select.return_value = mock_customer_df
    mock_customer_df.filter.return_value = mock_customer_df
    mock_customer_df.withColumn.return_value = mock_customer_df
    mock_customer_df.alias.return_value = mock_customer_df
    
    mock_order_df.join.return_value = mock_order_df
    mock_order_df.select.return_value = mock_order_df
    mock_order_df.filter.return_value = mock_order_df
    mock_order_df.withColumn.return_value = mock_order_df
    
    mock_ordersummary_df.filter.return_value = mock_ordersummary_df
    mock_ordersummary_df.groupBy.return_value.agg.return_value = mock_ordersummary_df
    mock_ordersummary_df.withColumnRenamed.return_value = mock_ordersummary_df
    mock_ordersummary_df.select.return_value = mock_ordersummary_df
    
    # Set up mock spark
    mock_spark.read.option.return_value.option.return_value.csv.return_value = mock_customer_df
    mock_spark.table.return_value = mock_ordersummary_df

@patch.dict('sys.modules', {'dlt': MockDLT()})
def test_dlt_customer_table(setup_mocks):
    # Import the module after mocking dlt
    from src.dlt_pipeline import customer
    
    # Set the global spark variable
    global spark
    spark = mock_spark
    
    # Call the function
    result = customer()
    
    # Verify the function was called correctly
    mock_spark.read.option.assert_called_with("header", "true")
    assert result is not None

@patch.dict('sys.modules', {'dlt': MockDLT()})
def test_dlt_order_table(setup_mocks):
    # Import the module after mocking dlt
    from src.dlt_pipeline import order
    
    # Set the global spark variable
    global spark
    spark = mock_spark
    
    # Call the function
    result = order()
    
    # Verify the function was called correctly
    mock_spark.read.option.assert_called_with("header", "true")
    assert result is not None

@patch.dict('sys.modules', {'dlt': MockDLT()})
@patch('src.dlt_pipeline.dlt', new_callable=lambda: MockDLT())
def test_dlt_customeraggregatespend(mock_dlt, setup_mocks):
    # Import the module after mocking dlt
    from src.dlt_pipeline import customeraggregatespend
    
    # Call the function
    result = customeraggregatespend()
    
    # Verify the expected operations were performed
    mock_ordersummary_df.filter.assert_called_once()
    assert result is not None