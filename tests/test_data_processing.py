"""
Unit tests for data processing functions.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
from src.data_processing import clean_customer_data, process_order_data, join_customer_order_data, aggregate_customer_spend

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestDataProcessing") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture(scope="module")
def sample_customer_data(spark):
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
        ("C004", None, "alice@example.com", "West"),
        ("C005", "Tom Brown", None, "North"),
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def sample_order_data(spark):
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    data = [
        ("O001", "Laptop", 1000.0, 1, datetime.date(2023, 1, 15), "C001"),
        ("O002", "Mouse", 25.0, 2, datetime.date(2023, 1, 16), "C002"),
        ("O003", "Keyboard", 50.0, 1, datetime.date(2023, 1, 17), "C003"),
        ("O004", "Monitor", 200.0, None, datetime.date(2023, 1, 18), "C001"),
        ("O005", "Headphones", 75.0, 1, datetime.date(2023, 1, 19), None),
        ("O001", "Laptop", 1000.0, 1, datetime.date(2023, 1, 15), "C001")  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def sample_scd_data(spark):
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("TotalAmount", DoubleType(), True),
        StructField("IsActive", StringType(), True),
        StructField("StartDate", DateType(), True),
        StructField("EndDate", DateType(), True)
    ])
    
    data = [
        ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1000.0, 1, 
         datetime.date(2023, 1, 15), 1000.0, "true", datetime.date(2023, 1, 1), None),
        ("C002", "Jane Smith", "jane@example.com", "South", "O002", "Mouse", 25.0, 2, 
         datetime.date(2023, 1, 16), 50.0, "true", datetime.date(2023, 1, 1), None),
        ("C003", "Bob Johnson", "bob@example.com", "East", "O003", "Keyboard", 50.0, 1, 
         datetime.date(2023, 1, 17), 50.0, "true", datetime.date(2023, 1, 1), None)
    ]
    
    return spark.createDataFrame(data, schema)

def test_clean_customer_data(spark, sample_customer_data):
    # Test cleaning customer data
    cleaned_df = clean_customer_data(sample_customer_data)
    
    # Check that nulls are removed
    assert cleaned_df.filter("Name IS NULL OR EmailId IS NULL OR Region IS NULL").count() == 0
    
    # Check that duplicates are removed
    assert cleaned_df.count() == 3
    
    # Check that the right records remain
    cust_ids = [row.CustId for row in cleaned_df.select("CustId").collect()]
    assert set(cust_ids) == {"C001", "C002", "C003"}

def test_process_order_data(spark, sample_order_data):
    # Test processing order data
    processed_df = process_order_data(sample_order_data)
    
    # Check that nulls are removed
    assert processed_df.filter("OrderId IS NULL OR ItemName IS NULL OR PricePerUnit IS NULL OR Qty IS NULL OR Date IS NULL OR CustId IS NULL").count() == 0
    
    # Check that duplicates are removed
    assert processed_df.count() == 3
    
    # Check that TotalAmount is calculated correctly
    order_o002 = processed_df.filter("OrderId = 'O002'").first()
    assert order_o002.TotalAmount == 50.0  # 25.0 * 2

def test_join_customer_order_data(spark, sample_customer_data, sample_order_data):
    # Clean data first
    cleaned_customer = clean_customer_data(sample_customer_data)
    processed_order = process_order_data(sample_order_data)
    
    # Test joining customer and order data
    joined_df = join_customer_order_data(cleaned_customer, processed_order)
    
    # Check that join worked correctly
    assert joined_df.count() == 3
    
    # Check that all required columns are present
    expected_columns = ["CustId", "Name", "EmailId", "Region", "OrderId", 
                       "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"]
    assert all(col in joined_df.columns for col in expected_columns)

def test_aggregate_customer_spend(spark, sample_scd_data):
    # Test aggregating customer spend
    agg_df = aggregate_customer_spend(sample_scd_data)
    
    # Check that aggregation worked correctly
    assert agg_df.count() == 3
    
    # Check that columns are correct
    assert agg_df.columns == ["Name", "TotalAmount", "Date"]
    
    # Check a specific aggregation result
    john_spend = agg_df.filter("Name = 'John Doe'").first()
    assert john_spend.TotalAmount == 1000.0