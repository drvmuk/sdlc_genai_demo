"""
Unit tests for utility functions.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from src.utils import get_spark_session, validate_data, apply_scd_type2_changes
import datetime

@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing.
    """
    return SparkSession.builder \
        .appName("TestUtils") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def sample_target_df(spark):
    """
    Create sample target DataFrame with SCD Type 2 columns.
    """
    data = [
        # CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date, IsActive, StartDate, EndDate
        ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1200.0, 1, 
         datetime.date(2023, 1, 15), True, datetime.datetime(2023, 1, 15, 10, 0), None),
        ("C002", "Jane Smith", "jane@example.com", "South", "O002", "Phone", 800.0, 2, 
         datetime.date(2023, 1, 20), True, datetime.datetime(2023, 1, 20, 10, 0), None),
        ("C003", "Bob Old", "bob@example.com", "East", "O003", "Tablet", 500.0, 1, 
         datetime.date(2023, 1, 25), True, datetime.datetime(2023, 1, 25, 10, 0), None)
    ]
    
    columns = ["CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", 
               "PricePerUnit", "Qty", "Date", "IsActive", "StartDate", "EndDate"]
    
    return spark.createDataFrame(data, columns)

@pytest.fixture
def sample_source_df(spark):
    """
    Create sample source DataFrame with changes.
    """
    data = [
        # CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date
        ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15)),
        ("C002", "Jane Updated", "jane@example.com", "South", "O002", "Phone", 800.0, 2, datetime.date(2023, 1, 20)),  # Name changed
        ("C003", "Bob Johnson", "bob@example.com", "East", "O003", "Tablet", 500.0, 1, datetime.date(2023, 1, 25)),  # Name changed
        ("C004", "Alice Brown", "alice@example.com", "West", "O004", "Headphones", 100.0, 3, datetime.date(2023, 1, 30))  # New record
    ]
    
    columns = ["CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", "PricePerUnit", "Qty", "Date"]
    
    return spark.createDataFrame(data, columns)

def test_get_spark_session():
    """
    Test get_spark_session function.
    """
    spark = get_spark_session()
    assert spark is not None
    assert isinstance(spark, SparkSession)

def test_validate_data(spark):
    """
    Test validate_data function.
    """
    # Create test DataFrame
    data = [("1", "John"), ("2", "Jane")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    # Test with valid columns
    assert validate_data(df, ["id", "name"]) == True
    
    # Test with invalid columns
    with pytest.raises(ValueError):
        validate_data(df, ["id", "nonexistent"])

def test_apply_scd_type2_changes(spark, sample_target_df, sample_source_df):
    """
    Test apply_scd_type2_changes function.
    """
    # Apply SCD Type 2 changes
    result = apply_scd_type2_changes(
        sample_target_df,
        sample_source_df,
        join_columns=["CustId", "OrderId"],
        compare_columns=["Name", "EmailId", "Region"]
    )
    
    # Check records to expire
    to_expire = result["to_expire"]
    assert to_expire.count() == 2  # C002 and C003 should be expired
    
    expired_custids = [row["CustId"] for row in to_expire.select("CustId").collect()]
    assert "C002" in expired_custids
    assert "C003" in expired_custids
    
    # Check all expired records have IsActive=False
    assert to_expire.filter(col("IsActive") == True).count() == 0
    
    # Check records to insert
    to_insert = result["to_insert"]
    assert to_insert.count() == 3  # C002 (updated), C003 (updated), and C004 (new)
    
    insert_custids = [row["CustId"] for row in to_insert.select("CustId").collect()]
    assert "C002" in insert_custids
    assert "C003" in insert_custids
    assert "C004" in insert_custids
    
    # Check all new records have IsActive=True
    assert to_insert.filter(col("IsActive") == False).count() == 0