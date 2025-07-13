"""
Unit tests for the Delta Live Tables pipeline.
"""

import pytest
from pyspark.sql import SparkSession
import sys
import os
from datetime import datetime

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

# Import the functions to test
from dlt_pipeline import customer_table, order_table, ordersummary_table, customeraggregatespend_table


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    """
    return SparkSession.builder \
        .appName("DLT-Pipeline-Test") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


@pytest.fixture
def sample_customer_data(spark):
    """
    Create sample customer data for testing.
    """
    data = [
        (1, "John Doe", "john@example.com", "North"),
        (2, "Jane Smith", "jane@example.com", "South"),
        (3, "Bob Johnson", "bob@example.com", "East"),
        (4, "Alice Brown", "alice@example.com", "West"),
        (5, None, "invalid@example.com", "North")  # Invalid record with null name
    ]
    columns = ["CustId", "Name", "EmailId", "Region"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_order_data(spark):
    """
    Create sample order data for testing.
    """
    data = [
        (101, "Item1", 10.0, 2, "2023-01-15", 1),
        (102, "Item2", 15.0, 1, "2023-01-20", 2),
        (103, "Item3", 20.0, 3, "2023-02-05", 3),
        (104, "Item4", 5.0, 5, "2023-02-10", 4),
        (105, "Item5", None, 2, "2023-03-01", 1)  # Invalid record with null price
    ]
    columns = ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
    return spark.createDataFrame(data, columns)


def test_customer_table_cleaning(spark, sample_customer_data, monkeypatch):
    """
    Test that the customer_table function properly cleans data.
    """
    # Mock the spark.read to return our sample data
    def mock_read(*args, **kwargs):
        return type('obj', (object,), {
            'option': lambda *a, **k: type('obj', (object,), {
                'option': lambda *a, **k: type('obj', (object,), {
                    'csv': lambda *a, **k: sample_customer_data
                })
            })
        })
    
    monkeypatch.setattr(spark, 'read', mock_read)
    
    # Mock the dlt.table decorator
    def mock_dlt_table(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    import dlt_pipeline
    monkeypatch.setattr(dlt_pipeline, 'spark', spark)
    monkeypatch.setattr(dlt_pipeline, 'dlt', type('obj', (object,), {'table': mock_dlt_table}))
    
    # Call the function
    result = customer_table()
    
    # Check that invalid records are removed
    assert result.count() == 4
    assert all(row["Name"] is not None for row in result.collect())


def test_order_table_transformations(spark, sample_order_data, monkeypatch):
    """
    Test that the order_table function properly transforms data.
    """
    # Mock the spark.read to return our sample data
    def mock_read(*args, **kwargs):
        return type('obj', (object,), {
            'option': lambda *a, **k: type('obj', (object,), {
                'option': lambda *a, **k: type('obj', (object,), {
                    'csv': lambda *a, **k: sample_order_data
                })
            })
        })
    
    monkeypatch.setattr(spark, 'read', mock_read)
    
    # Mock the dlt.table decorator
    def mock_dlt_table(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    import dlt_pipeline
    monkeypatch.setattr(dlt_pipeline, 'spark', spark)
    monkeypatch.setattr(dlt_pipeline, 'dlt', type('obj', (object,), {'table': mock_dlt_table}))
    
    # Call the function
    result = order_table()
    
    # Check that invalid records are removed and TotalAmount is calculated
    assert result.count() == 4
    
    # Check that TotalAmount is calculated correctly
    for row in result.collect():
        assert row["TotalAmount"] == row["PricePerUnit"] * row["Qty"]


def test_ordersummary_join(spark, sample_customer_data, sample_order_data, monkeypatch):
    """
    Test that the ordersummary_table function properly joins data.
    """
    # Clean the sample data as the functions would
    clean_customer = sample_customer_data.filter(
        (sample_customer_data["CustId"].isNotNull()) &
        (sample_customer_data["Name"].isNotNull()) &
        (sample_customer_data["EmailId"].isNotNull()) &
        (sample_customer_data["Region"].isNotNull())
    ).dropDuplicates(["CustId"])
    
    clean_order = sample_order_data.filter(
        (sample_order_data["OrderId"].isNotNull()) &
        (sample_order_data["ItemName"].isNotNull()) &
        (sample_order_data["PricePerUnit"].isNotNull()) &
        (sample_order_data["Qty"].isNotNull()) &
        (sample_order_data["Date"].isNotNull()) &
        (sample_order_data["CustId"].isNotNull())
    ).dropDuplicates(["OrderId"])
    
    # Mock dlt.read to return our cleaned sample data
    def mock_dlt_read(table_name):
        if table_name == "customer":
            return clean_customer
        elif table_name == "order":
            return clean_order
        else:
            raise ValueError(f"Unknown table: {table_name}")
    
    # Mock the dlt.table decorator
    def mock_dlt_table(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    # Mock spark.table to raise an exception (simulate table not existing)
    def mock_spark_table(*args, **kwargs):
        raise Exception("Table not found")
    
    import dlt_pipeline
    monkeypatch.setattr(dlt_pipeline, 'spark', type('obj', (object,), {'table': mock_spark_table}))
    monkeypatch.setattr(dlt_pipeline, 'dlt', type('obj', (object,), {'table': mock_dlt_table, 'read': mock_dlt_read}))
    
    # Call the function
    result = ordersummary_table()
    
    # Check that the join worked correctly
    assert result.count() == 4  # All valid records should be joined
    
    # Check that SCD Type 2 columns are added
    assert "StartDate" in result.columns
    assert "EndDate" in result.columns
    assert "IsActive" in result.columns
    
    # Check that all records are active
    assert all(row["IsActive"] for row in result.collect())


def test_customeraggregatespend_aggregation(spark, monkeypatch):
    """
    Test that the customeraggregatespend_table function properly aggregates data.
    """
    # Create sample ordersummary data
    current_date = datetime.now().strftime("%Y-%m-%d")
    data = [
        ("John Doe", "2023-01-15", 20.0, current_date, "9999-12-31", True),
        ("John Doe", "2023-01-15", 30.0, current_date, "9999-12-31", True),
        ("Jane Smith", "2023-01-20", 15.0, current_date, "9999-12-31", True),
        ("Bob Johnson", "2023-02-05", 60.0, current_date, "9999-12-31", True),
        ("John Doe", "2023-01-15", 25.0, current_date, "2023-03-01", False)  # Inactive record
    ]
    columns = ["Name", "Date", "TotalAmount", "StartDate", "EndDate", "IsActive"]
    ordersummary_df = spark.createDataFrame(data, columns)
    
    # Mock dlt.read to return our sample data
    def mock_dlt_read(table_name):
        if table_name == "ordersummary":
            return ordersummary_df
        else:
            raise ValueError(f"Unknown table: {table_name}")
    
    # Mock the dlt.table decorator
    def mock_dlt_table(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    import dlt_pipeline
    monkeypatch.setattr(dlt_pipeline, 'dlt', type('obj', (object,), {'table': mock_dlt_table, 'read': mock_dlt_read}))
    
    # Call the function
    result = customeraggregatespend_table()
    
    # Check that aggregation worked correctly
    assert result.count() == 3  # 3 unique Name-Date combinations in active records
    
    # Check specific aggregation results
    result_dict = {(row["Name"], row["Date"]): row["TotalSpend"] for row in result.collect()}
    assert result_dict[("John Doe", "2023-01-15")] == 50.0  # 20.0 + 30.0
    assert result_dict[("Jane Smith", "2023-01-20")] == 15.0
    assert result_dict[("Bob Johnson", "2023-02-05")] == 60.0