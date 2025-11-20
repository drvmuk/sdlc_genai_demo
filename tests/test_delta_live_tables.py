import pytest
import os
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime

# Mock the dlt module since it's only available in Databricks
class MockDLT:
    @staticmethod
    def table(*args, **kwargs):
        def decorator(f):
            return f
        return decorator
    
    @staticmethod
    def read(table_name):
        # This is a simplified mock - in real tests you'd want to return appropriate DataFrames
        if table_name == "customer":
            return mock_customer_df
        elif table_name == "order":
            return mock_order_df
        elif table_name == "ordersummary":
            return mock_ordersummary_df
        return None

# Create mock DataFrames that will be used in tests
mock_customer_df = None
mock_order_df = None
mock_ordersummary_df = None

# Mock the dlt module
mock_dlt = MockDLT()

# Patch the dlt module
@pytest.fixture(autouse=True)
def mock_dlt_module():
    with patch.dict('sys.modules', {'dlt': mock_dlt}):
        yield

@pytest.fixture
def spark():
    """
    Create a SparkSession for testing
    """
    return SparkSession.builder \
        .appName("TestDeltaLiveTables") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def setup_mock_dataframes(spark):
    """
    Set up mock DataFrames for testing
    """
    global mock_customer_df, mock_order_df, mock_ordersummary_df
    
    # Create customer DataFrame
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    customer_data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West")
    ]
    
    mock_customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Create order DataFrame
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", StringType(), True),
        StructField("TotalAmount", DoubleType(), True)
    ])
    
    order_data = [
        ("O001", "Product A", 10.0, 2, "2023-01-15", "C001", 20.0),
        ("O002", "Product B", 15.0, 1, "2023-01-20", "C002", 15.0),
        ("O003", "Product C", 20.0, 3, "2023-01-25", "C003", 60.0),
        ("O004", "Product D", 25.0, 1, "2023-01-30", "C004", 25.0)
    ]
    
    mock_order_df = spark.createDataFrame(order_data, order_schema)
    
    # Create ordersummary DataFrame
    ordersummary_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("TotalAmount", DoubleType(), True),
        StructField("IsActive", StringType(), True),
        StructField("StartDate", StringType(), True),
        StructField("EndDate", StringType(), True)
    ])
    
    ordersummary_data = [
        ("C001", "John Doe", "john@example.com", "North", "O001", "Product A", 10.0, 2, "2023-01-15", 20.0, "true", "2023-01-15", None),
        ("C002", "Jane Smith", "jane@example.com", "South", "O002", "Product B", 15.0, 1, "2023-01-20", 15.0, "true", "2023-01-20", None),
        ("C003", "Bob Johnson", "bob@example.com", "East", "O003", "Product C", 20.0, 3, "2023-01-25", 60.0, "true", "2023-01-25", None),
        ("C004", "Alice Brown", "alice@example.com", "West", "O004", "Product D", 25.0, 1, "2023-01-30", 25.0, "true", "2023-01-30", None)
    ]
    
    mock_ordersummary_df = spark.createDataFrame(ordersummary_data, ordersummary_schema)

def test_import_delta_live_tables(setup_mock_dataframes):
    """
    Test that we can import the delta_live_tables module
    """
    from src.delta_live_tables import customer, order, ordersummary, customeraggregatespend
    
    # If import succeeds, the test passes
    assert True

def test_customer_function(setup_mock_dataframes, spark, monkeypatch):
    """
    Test the customer function in delta_live_tables
    """
    # Mock spark.read to return our test DataFrame
    mock_read = MagicMock()
    mock_read.format.return_value.option.return_value.option.return_value.load.return_value = mock_customer_df
    monkeypatch.setattr(spark, 'read', mock_read)
    
    # Import and test the function
    from src.delta_live_tables import customer
    result_df = customer()
    
    # Check that the function returns a DataFrame
    assert result_df is not None
    
    # Check that the DataFrame has the expected columns
    expected_columns = ["CustId", "Name", "EmailId", "Region"]
    assert all(col in result_df.columns for col in expected_columns)

def test_order_function(setup_mock_dataframes, spark, monkeypatch):
    """
    Test the order function in delta_live_tables
    """
    # Create a DataFrame without TotalAmount to simulate raw input
    order_data_raw = [
        ("O001", "Product A", 10.0, 2, "2023-01-15", "C001"),
        ("O002", "Product B", 15.0, 1, "2023-01-20", "C002"),
        ("O003", "Product C", 20.0, 3, "2023-01-25", "C003"),
        ("O004", "Product D", 25.0, 1, "2023-01-30", "C004")
    ]
    
    order_schema_raw = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    raw_order_df = spark.createDataFrame(order_data_raw, order_schema_raw)
    
    # Mock spark.read to return our test DataFrame
    mock_read = MagicMock()
    mock_read.format.return_value.option.return_value.option.return_value.load.return_value = raw_order_df
    monkeypatch.setattr(spark, 'read', mock_read)
    
    # Import and test the function
    from src.delta_live_tables import order
    result_df = order()
    
    # Check that the function returns a DataFrame
    assert result_df is not None
    
    # Check that TotalAmount column was added
    assert "TotalAmount" in result_df.columns
    
    # Verify TotalAmount calculation for a sample row
    sample_row = result_df.filter("OrderId = 'O001'").first()
    assert sample_row["TotalAmount"] == sample_row["PricePerUnit"] * sample_row["Qty"]