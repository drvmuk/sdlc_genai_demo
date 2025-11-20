import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
from src.batch_processing import (
    clean_customer_data,
    clean_order_data,
    create_or_update_ordersummary,
    create_or_update_customeraggregatespend
)

@pytest.fixture
def spark():
    """
    Create a SparkSession for testing
    """
    return SparkSession.builder \
        .appName("TestCustomerOrderProcessing") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def sample_customer_data(spark):
    """
    Create sample customer data for testing
    """
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", None, "invalid@example.com", "North"),
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_order_data(spark):
    """
    Create sample order data for testing
    """
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    data = [
        ("O001", "Product A", 10.0, 2, "2023-01-15", "C001"),
        ("O002", "Product B", 15.0, 1, "2023-01-20", "C002"),
        ("O003", "Product C", 20.0, 3, "2023-01-25", "C003"),
        ("O004", "Product D", 25.0, 1, "2023-01-30", "C004"),
        ("O005", "Product E", 30.0, None, "2023-02-05", "C001"),
        ("O001", "Product A", 10.0, 2, "2023-01-15", "C001")  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

def test_clean_customer_data(spark, sample_customer_data):
    """
    Test cleaning customer data
    """
    cleaned_df = clean_customer_data(sample_customer_data)
    
    # Check if nulls are removed
    assert cleaned_df.filter("Name IS NULL").count() == 0
    
    # Check if duplicates are removed
    assert cleaned_df.count() == 4
    
    # Check if all required columns exist
    required_columns = ["CustId", "Name", "EmailId", "Region"]
    assert all(col in cleaned_df.columns for col in required_columns)

def test_clean_order_data(spark, sample_order_data):
    """
    Test cleaning order data and adding TotalAmount column
    """
    cleaned_df = clean_order_data(sample_order_data)
    
    # Check if nulls are removed
    assert cleaned_df.filter("Qty IS NULL").count() == 0
    
    # Check if duplicates are removed
    assert cleaned_df.count() == 4
    
    # Check if TotalAmount column is added
    assert "TotalAmount" in cleaned_df.columns
    
    # Verify TotalAmount calculation
    row = cleaned_df.filter("OrderId = 'O001'").first()
    assert row["TotalAmount"] == row["PricePerUnit"] * row["Qty"]