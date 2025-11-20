import pytest
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from chispa.dataframe_comparer import assert_df_equality
from src.batch_pipeline import process_order_data, clean_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestBatchPipeline") \
        .master("local[2]") \
        .getOrCreate()

def test_process_order_data(spark):
    # Create sample order data
    order_data = [
        ("O001", "Item1", 10.0, 2, datetime.date(2023, 1, 1), "C001"),
        ("O002", "Item2", 15.0, 3, datetime.date(2023, 1, 2), "C002"),
        ("O003", "Item3", 5.0, 5, datetime.date(2023, 1, 3), "C001"),
        ("O004", "Item4", None, 2, datetime.date(2023, 1, 4), "C002"),  # Contains null
        ("O005", "Item5", 20.0, None, datetime.date(2023, 1, 5), "C003")  # Contains null
    ]
    
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    order_df = spark.createDataFrame(order_data, order_schema)
    
    # Process order data
    result_df = process_order_data(order_df)
    
    # Expected results (nulls removed, TotalAmount calculated)
    expected_data = [
        ("O001", "Item1", 10.0, 2, datetime.date(2023, 1, 1), "C001", 20.0),
        ("O002", "Item2", 15.0, 3, datetime.date(2023, 1, 2), "C002", 45.0),
        ("O003", "Item3", 5.0, 5, datetime.date(2023, 1, 3), "C001", 25.0)
    ]
    
    expected_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True),
        StructField("TotalAmount", DoubleType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Compare results (ignoring order)
    assert result_df.count() == expected_df.count()
    assert set(result_df.select("OrderId", "TotalAmount").collect()) == \
           set(expected_df.select("OrderId", "TotalAmount").collect())

def test_clean_data(spark):
    # Create sample data with nulls and duplicates
    data = [
        ("C001", "John", "john@example.com", "North"),
        ("C002", "Jane", "jane@example.com", "South"),
        ("C002", "Jane", "jane@example.com", "South"),  # Duplicate
        ("C003", None, "bob@example.com", "East"),      # Contains null
        ("C004", "Alice", None, "West"),                # Contains null
        ("C005", "Charlie", "charlie@example.com", "North")
    ]
    
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)