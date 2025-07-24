"""
Unit tests for custom DQX rules.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.dq_rules import dqx_null_check, dqx_primary_check

@pytest.fixture(scope="module")
def spark():
    """
    Create a Spark session for testing.
    """
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("DQXRulesTest")
        .getOrCreate()
    )

@pytest.fixture(scope="module")
def sample_df(spark):
    """
    Create a sample DataFrame for testing.
    """
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    
    data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (3, None, 40),
        (4, "Dave", None),
        (5, "Eve", 35),
        (5, "Duplicate", 50)  # Duplicate ID for testing primary key check
    ]
    
    return spark.createDataFrame(data, schema)

def test_dqx_null_check(spark, sample_df):
    """
    Test the dqx_null_check function.
    """
    # Test with a column that has nulls
    condition, message, function_name = dqx_null_check(sample_df, ["name"])
    assert function_name == "dqx_null_check"
    assert "should not contain NULL values" in message
    
    # The condition should be false since 'name' has nulls
    condition_result = condition.collect()[0][0]
    assert condition_result == False
    
    # Test with a column that has no nulls
    condition, message, function_name = dqx_null_check(sample_df, ["id"])
    condition_result = condition.collect()[0][0]
    assert condition_result == True

def test_dqx_null_check_multiple_columns(spark, sample_df):
    """
    Test the dqx_null_check function with multiple columns.
    """
    # Test with multiple columns where some have nulls
    condition, message, function_name = dqx_null_check(sample_df, ["id", "name", "age"])
    condition_result = condition.collect()[0][0]
    assert condition_result == False
    
    # Test with columns that have no nulls
    filtered_df = sample_df.filter(sample_df.name.isNotNull() & sample_df.age.isNotNull())
    condition, message, function_name = dqx_null_check(filtered_df, ["id", "name", "age"])
    condition_result = condition.collect()[0][0]
    assert condition_result == True

def test_dqx_primary_check(spark, sample_df):
    """
    Test the dqx_primary_check function.
    """
    # Test with a column that should be unique but has duplicates
    condition, message, function_name = dqx_primary_check(sample_df, ["id"])
    assert function_name == "dqx_primary_check"
    assert "should form a unique key" in message
    
    # The condition should be false since 'id' has duplicates
    condition_result = condition.collect()[0][0]
    assert condition_result == False
    
    # Test with a combination of columns that form a unique key
    condition, message, function_name = dqx_primary_check(sample_df, ["id", "name"])
    condition_result = condition.collect()[0][0]
    assert condition_result == True

def test_dqx_null_check_invalid_column(spark, sample_df):
    """
    Test the dqx_null_check function with an invalid column.
    """
    with pytest.raises(ValueError):
        dqx_null_check(sample_df, ["invalid_column"])

def test_dqx_primary_check_invalid_column(spark, sample_df):
    """
    Test the dqx_primary_check function with an invalid column.
    """
    with pytest.raises(ValueError):
        dqx_primary_check(sample_df, ["invalid_column"])