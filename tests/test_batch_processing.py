"""
Unit tests for the batch processing module.
"""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import date, datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, BooleanType, TimestampType

from src.batch_processing import (
    process_customer_data,
    process_order_data,
    create_order_summary_scd2,
    create_customer_aggregate_spend
)

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("Unit Tests") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture(scope="module")
def sample_customer_data(spark):
    """Create sample customer data for testing."""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        (None, "Invalid", "invalid@example.com", "West"),  # Should be filtered out
        ("C004", None, "missing@example.com", "North"),    # Should be filtered out
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate, should be removed
    ]
    
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def sample_order_data(spark):
    """Create sample order data for testing."""
    data = [
        ("O001", "Laptop", 1000.0, 2, date(2023, 1, 15), "C001"),
        ("O002", "Phone", 500.0, 1, date(2023, 1, 20), "C002"),
        ("O003", "Tablet", 300.0, 3, date(2023, 1, 25), "C003"),
        ("O004", "Monitor", 200.0, 2, date(2023, 1, 30), "C001"),
        (None, "Invalid", 100.0, 1, date(2023, 2, 5), "C002"),    # Should be filtered out
        ("O005", "Keyboard", 50.0, None, date(2023, 2, 10), "C003"),  # Should be filtered out
        ("O001", "Laptop", 1000.0, 2, date(2023, 1, 15), "C001")  # Duplicate, should be removed
    ]
    
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

def test_process_customer_data(spark, sample_customer_data, monkeypatch):
    """Test customer data processing logic."""
    # Mock the read function to return our sample data
    def mock_read(*args, **kwargs):
        return sample_customer_data
    
    # Mock the write function to do nothing
    class MockWriter:
        def format(self, *args, **kwargs):
            return self
        def mode(self, *args, **kwargs):
            return self
        def saveAsTable(self, *args, **kwargs):
            pass
    
    class MockDataFrame:
        def write(self):
            return MockWriter()
    
    monkeypatch.setattr(spark, "read", mock_read)
    monkeypatch.setattr(sample_customer_data, "write", MockWriter().format)
    
    # Process the data
    result_df = process_customer_data(spark)
    
    # Check the results
    assert result_df.count() == 3  # Should have 3 valid records after cleaning
    assert len(result_df.columns) == 4
    
    # Check that null values were removed
    null_count = result_df.filter(
        F.col("CustId").isNull() | 
        F.col("Name").isNull() | 
        F.col("EmailId").isNull() | 
        F.col("Region").isNull()
    ).count()
    assert null_count == 0
    
    # Check that duplicates were removed
    distinct_count = result_df.select("CustId").distinct().count()
    assert distinct_count == 3

def test_process_order_data(spark, sample_order_data, monkeypatch):
    """Test order data processing logic."""
    # Mock the read function to return our sample data
    def mock_read(*args, **kwargs):
        return sample_order_data
    
    # Mock the write function to do nothing
    class MockWriter:
        def format(self, *args, **kwargs):
            return self
        def mode(self, *args, **kwargs):
            return self
        def saveAsTable(self, *args, **kwargs):
            pass
    
    monkeypatch.setattr(spark, "read", mock_read)
    monkeypatch.setattr(sample_order_data, "write", MockWriter().format)
    
    # Process the data
    result_df = process_order_data(spark)
    
    # Check the results
    assert result_df.count() == 4  # Should have 4 valid records after cleaning
    assert len(result_df.columns) == 7  # Original 6 columns + TotalAmount
    
    # Check that null values were removed
    null_count = result_df.filter(
        F.col("OrderId").isNull() | 
        F.col("ItemName").isNull() | 
        F.col("PricePerUnit").isNull() | 
        F.col("Qty").isNull() | 
        F.col("Date").isNull() | 
        F.col("CustId").isNull()
    ).count()
    assert null_count == 0
    
    # Check that duplicates were removed
    distinct_count = result_df.select("OrderId").distinct().count()
    assert distinct_count == 4