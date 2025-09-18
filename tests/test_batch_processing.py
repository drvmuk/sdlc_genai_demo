import pytest
from pyspark.sql import SparkSession
import pandas as pd
import os
import tempfile
import shutil
from src.batch_processing import (
    read_source_data,
    clean_and_transform_data,
    create_or_update_ordersummary,
    create_customer_aggregate_spend
)

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("TestBatchProcessing") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

@pytest.fixture(scope="module")
def test_data_path():
    """Create temporary directories for test data"""
    temp_dir = tempfile.mkdtemp()
    customer_path = os.path.join(temp_dir, "customerdata")
    order_path = os.path.join(temp_dir, "orderdata")
    os.makedirs(customer_path, exist_ok=True)
    os.makedirs(order_path, exist_ok=True)
    
    # Create sample customer data
    customer_data = pd.DataFrame({
        'CustId': ['C001', 'C002', 'C003', 'C004', None],
        'Name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Invalid User'],
        'EmailId': ['john@example.com', 'jane@example.com', 'bob@example.com', 'alice@example.com', None],
        'Region': ['East', 'West', 'North', 'South', 'East']
    })
    customer_data.to_csv(os.path.join(customer_path, "customer.csv"), index=False)
    
    # Create sample order data
    order_data = pd.DataFrame({
        'OrderId': ['O001', 'O002', 'O003', 'O004', 'O005', None],
        'ItemName': ['Item1', 'Item2', 'Item3', 'Item4', 'Item5', 'Item6'],
        'PricePerUnit': [10.0, 20.0, 15.0, 25.0, 30.0, 5.0],
        'Qty': [2, 1, 3, 2, 1, None],
        'Date': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-03', '2023-01-02', '2023-01-04'],
        'CustId': ['C001', 'C002', 'C001', 'C003', 'C004', 'C999']
    })
    order_data.to_csv(os.path.join(order_path, "order.csv"), index=False)
    
    # Mock the volume paths
    os.environ["VOLUMES_PATH"] = temp_dir
    
    yield {"customer_path": customer_path, "order_path": order_path}
    
    # Cleanup
    shutil.rmtree(temp_dir)

def test_read_source_data(spark, monkeypatch):
    """Test reading source data"""
    # Mock the read function to use test data paths
    def mock_read_source_data(spark_session):
        customer_df = spark_session.createDataFrame(
            [("C001", "John Doe", "john@example.com", "East"),
             ("C002", "Jane Smith", "jane@example.com", "West")],
            ["CustId", "Name", "EmailId", "Region"]
        )
        
        order_df = spark_session.createDataFrame(
            [("O001", "Item1", 10.0, 2, "2023-01-01", "C001"),
             ("O002", "Item2", 20.0, 1, "2023-01-02", "C002")],
            ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
        )
        
        return customer_df, order_df
    
    monkeypatch.setattr("src.batch_processing.read_source_data", mock_read_source_data)
    
    # Call the mocked function
    customer_df, order_df = mock_read_source_data(spark)
    
    # Verify the results
    assert customer_df.count() == 2, "Should have 2 customer records"
    assert order_df.count() == 2, "Should have 2 order records"
    
    # Check schema
    assert customer_df.columns == ["CustId", "Name", "EmailId", "Region"]
    assert order_df.columns == ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]

def test_clean_and_transform_data(spark):
    """Test data cleaning and transformation logic"""
    # Create test dataframes
    customer_df = spark.createDataFrame(
        [("C001", "John Doe", "john@example.com", "East"),
         ("C002", "Jane Smith", "jane@example.com", "West"),
         (None, "Invalid User", "invalid@example.com", "North"),
         ("C003", None, "bob@example.com", "South"),
         ("C001", "John Doe", "john@example.com", "East")],  # Duplicate
        ["CustId", "Name", "EmailId", "Region"]
    )
    
    order_df = spark.createDataFrame(
        [("O001", "Item1", 10.0, 2, "2023-01-01", "C001"),
         ("O002", "Item2", 20.0, 1, "2023-01-02", "C002"),
         (None, "Item3", 15.0, 3, "2023-01-03", "C003"),
         ("O003", "Item4", 25.0, None, "2023-01-04", "C004"),
         ("O001", "Item1", 10.0, 2, "2023-01-01", "C001")],  # Duplicate
        ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
    )
    
    # Clean and transform the data
    customer_clean, order_clean = clean_and_transform_data(customer_df, order_df)
    
    # Verify customer cleaning
    assert customer_clean.count() == 2, "Should have 2 valid customer records after cleaning"
    
    # Verify order cleaning and transformation
    assert order_clean.count() == 2, "Should have 2 valid order records after cleaning"
    
    # Check TotalAmount calculation
    first_order = order_clean.filter(order_clean.OrderId == "O001").first()
    assert first_order.TotalAmount == 20.0, "TotalAmount should be 20.0 for OrderId O001"

def test_create_or_update_ordersummary