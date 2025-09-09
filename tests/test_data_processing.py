"""
Unit tests for the data processing module.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import datetime
from src.data_processing import clean_data

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("TestDataProcessing") \
        .master("local[1]") \
        .getOrCreate()

def test_clean_data(spark):
    """Test the clean_data function."""
    # Create a test dataframe with nulls and duplicates
    data = [
        ("1", "John", "john@example.com", "North"),
        ("2", "Jane", None, "South"),
        ("3", "Bob", "bob@example.com", "East"),
        ("1", "John", "john@example.com", "North"),  # Duplicate
    ]
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    # Clean the data
    cleaned_df = clean_data(df)
    
    # Check that nulls and duplicates are removed
    assert cleaned_df.count() == 2
    assert "John" in [row.Name for row in cleaned_df.collect()]
    assert "Bob" in [row.Name for row in cleaned_df.collect()]
    assert "Jane" not in [row.Name for row in cleaned_df.collect()]  # Row with null should be removed

def test_order_total_calculation(spark):
    """Test the calculation of TotalAmount in orders."""
    # Create a test order dataframe
    data = [
        ("O1", "Item1", 10.0, 2, datetime.date(2023, 1, 1), "C1"),
        ("O2", "Item2", 15.0, 3, datetime.date(2023, 1, 2), "C2"),
    ]
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    # Calculate TotalAmount
    from pyspark.sql.functions import col
    df_with_total = df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    # Check the calculations
    results = {row.OrderId: row.TotalAmount for row in df_with_total.collect()}
    assert results["O1"] == 20.0  # 10.0 * 2
    assert results["O2"] == 45.0  # 15.0 * 3