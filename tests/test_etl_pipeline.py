import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
from src.etl_pipeline import clean_data, apply_scd_type2_changes

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("ETL Pipeline Tests") \
        .master("local[1]") \
        .getOrCreate()

def test_clean_data(spark):
    """Test the clean_data function."""
    # Create test data with nulls and duplicates
    data = [
        ("1", "John", "john@example.com", "North"),
        ("2", None, "jane@example.com", "South"),
        ("3", "Bob", "bob@example.com", "East"),
        ("3", "Bob", "bob@example.com", "East"),  # Duplicate
        ("4", "Alice", None, "West")
    ]
    
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Apply the clean_data function
    cleaned_df = clean_data(df)
    
    # Check that nulls and duplicates are removed
    assert cleaned_df.count() == 2
    assert cleaned_df.filter("Name IS NULL OR EmailId IS NULL OR Region IS NULL").count() == 0

def test_apply_scd_type2_changes(spark):
    """Test the apply_scd_type2_changes function."""
    # Create current data
    current_data = [
        ("1", "John", "North", True, datetime.datetime(2023, 1, 1), None),
        ("2", "Jane", "South", True, datetime.datetime(2023, 1, 1), None),
        ("3", "Bob", "East", False, datetime.datetime(2023, 1, 1), datetime.datetime(2023, 2, 1))
    ]
    
    current_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("IsActive", StringType(), True),
        StructField("StartDate", DateType(), True),
        StructField("EndDate", DateType(), True)
    ])
    
    current_df = spark.createDataFrame(current_data, current_schema)
    
    # Create new data with changes
    new_data = [
        ("1", "John", "North"),  # No change
        ("2", "Jane", "West"),   # Region changed
        ("4", "Alice", "Central")  # New record
    ]
    
    new_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    new_df = spark.createDataFrame(new_data, new_schema)
    
    # Apply SCD Type 2 changes
    result_df = apply_scd_type2_changes(
        current_df=current_df,
        new_df=new_df,
        key_columns=["CustId"],
        change_columns=["Name", "Region"]
    )
    
    # Check results
    # Should have 5 records total:
    # - 1 unchanged (CustId=1)
    # - 1 expired (CustId=2, IsActive=False)
    # - 1 new version of changed record (CustId=2, IsActive=True)
    # - 1 already expired record (CustId=3, IsActive=False)
    # - 1 completely new record (CustId=4, IsActive=True)
    assert result_df.count() == 5
    
    # Check that we have 3 active records (CustId 1, new version of 2, and 4)
    assert result_df.filter("IsActive = True").count() == 3
    
    # Check that we have 2 inactive records (old version of 2 and 3)
    assert result_df.filter("IsActive = False").count() == 2