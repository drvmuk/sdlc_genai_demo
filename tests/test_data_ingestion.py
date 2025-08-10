"""
Tests for the data ingestion module
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
import datetime

from src.data_ingestion import (
    create_customer_schema,
    create_order_schema,
    read_customer_data,
    read_order_data,
    write_to_delta_table
)

@pytest.fixture
def spark():
    """
    Create a Spark session for testing
    
    Returns:
        SparkSession: The Spark session
    """
    return SparkSession.builder \
        .appName("test-data-ingestion") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def test_create_customer_schema():
    """
    Test creating the customer schema
    """
    schema = create_customer_schema()
    
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 4
    assert schema.fields[0].name == "CustId"
    assert schema.fields[1].name == "Name"
    assert schema.fields[2].name == "EmailId"
    assert schema.fields[3].name == "Region"

def test_create_order_schema():
    """
    Test creating the order schema
    """
    schema = create_order_schema()
    
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 6
    assert schema.fields[0].name == "OrderId"
    assert schema.fields[1].name == "ItemName"
    assert schema.fields[2].name == "PricePerUnit"
    assert schema.fields[3].name == "Qty"
    assert schema.fields[4].name == "Date"
    assert schema.fields[5].name == "CustId"

def test_read_customer_data(spark, monkeypatch):
    """
    Test reading customer data
    
    Args:
        spark (SparkSession): The Spark session
        monkeypatch: Pytest monkeypatch fixture
    """
    # Create mock customer data
    customer_data = [
        (1, "John Doe", "john.doe@example.com", "North"),
        (2, "Jane Smith", "jane.smith@example.com", "South"),
        (3, "Bob Johnson", "bob.johnson@example.com", "East")
    ]
    
    customer_df = spark.createDataFrame(
        customer_data,
        ["CustId", "Name", "EmailId", "Region"]
    )
    
    # Mock the spark.read.format().option().schema().load() chain
    class MockDataFrameReader:
        def format(self, format_type):
            return self
            
        def option(self, key, value):
            return self
            
        def schema(self, schema):
            return self
            
        def load(self, path):
            return customer_df
    
    # Mock get_spark_session to return our test spark session
    def mock_get_spark_session():
        return spark
    
    # Apply the monkeypatches
    monkeypatch.setattr(spark, "read", MockDataFrameReader())
    monkeypatch.setattr("src.data_ingestion.get_spark_session", mock_get_spark_session)
    
    # Call the function
    result = read_customer_data()
    
    # Verify the result
    assert result.count() == 3
    assert result.columns == ["CustId", "Name", "EmailId", "Region"]

def test_read_order_data(spark, monkeypatch):
    """
    Test reading order data
    
    Args:
        spark (SparkSession): The Spark session
        monkeypatch: Pytest monkeypatch fixture
    """
    # Create mock order data
    order_data = [
        (101, "Product A", 10.5, 2, datetime.date(2023, 1, 15), 1),
        (102, "Product B", 20.0, 1, datetime.date(2023, 1, 16), 2),
        (103, "Product C", 15.75, 3, datetime.date(2023, 1, 17), 3)
    ]
    
    order_df = spark.createDataFrame(
        order_data,
        ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
    )
    
    # Mock the spark.read.format().option().schema().load() chain
    class MockDataFrameReader:
        def format(self, format_type):
            return self
            
        def option(self, key, value):
            return self
            
        def schema(self, schema):
            return self
            
        def load(self, path):
            return order_df
    
    # Mock get_spark_session to return our test spark session
    def mock_get_spark_session():
        return spark
    
    # Apply the monkeypatches
    monkeypatch.setattr(spark, "read", MockDataFrameReader())
    monkeypatch.setattr("src.data_ingestion.get_spark_session", mock_get_spark_session)
    
    # Call the function
    result = read_order_data()
    
    # Verify the result
    assert result.count() == 3
    assert result.columns == ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]

def test_write_to_delta_table(spark, monkeypatch):
    """
    Test writing data to a Delta table
    
    Args:
        spark (SparkSession): The Spark session
        monkeypatch: Pytest monkeypatch fixture
    """
    # Create mock data
    data = [
        (1, "John Doe", "john.doe@example.com", "North"),
        (2, "Jane Smith", "jane.smith@example.com", "South")
    ]
    
    df = spark.createDataFrame(
        data,
        ["CustId", "Name", "EmailId", "Region"]
    )
    
    # Track if write was called
    write_called = False
    
    # Mock the df.write.format().mode().saveAsTable() chain
    class MockDataFrameWriter:
        def format(self, format_type):
            assert format_type == "delta"
            return self
            
        def mode(self, write_mode):
            assert write_mode == "overwrite"
            return self
            
        def saveAsTable(self, table_name):
            nonlocal write_called
            write_called = True
            assert table_name == "test_table"
    
    # Apply the monkeypatch
    monkeypatch.setattr(df, "write", MockDataFrameWriter())
    
    # Call the function
    write_to_delta_table(df, "test_table")
    
    # Verify write was called
    assert write_called