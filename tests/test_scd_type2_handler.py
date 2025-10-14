import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime
import tempfile
import shutil
import os
from src.scd_type2_handler import SCDType2Handler

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return (
        SparkSession.builder
        .appName("SCDType2Tests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

@pytest.fixture(scope="function")
def temp_dir():
    """Create a temporary directory for Delta tables."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)

@pytest.fixture
def initial_customer_data(spark):
    """Create initial customer data for SCD Type 2 testing."""
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", "Jane Smith", "jane.smith@example.com", "South"),
        ("C003", "Bob Johnson", "bob.johnson@example.com", "East"),
    ]
    
    return spark.createDataFrame(data, ["CustId", "Name", "EmailId", "Region"])

@pytest.fixture
def updated_customer_data(spark):
    """Create updated customer data for SCD Type 2 testing."""
    data = [
        ("C001", "John Doe", "john.doe@example.com", "West"),  # Region changed from North to West
        ("C002", "Jane Smith", "jane.updated@example.com", "South"),  # Email changed
        ("C004", "Alice Brown", "alice.brown@example.com", "Central"),  # New customer
    ]
    
    return spark.createDataFrame(data, ["CustId", "Name", "EmailId", "Region"])

def test_scd_type2_initial_load(spark, temp_dir, initial_customer_data):
    """Test initial load into an SCD Type 2 table."""
    # Setup
    target_path = os.path.join(temp_dir, "customer_scd")
    handler = SCDType2Handler(spark)
    
    # Execute
    handler.merge_scd_type2(
        target_path,
        initial_customer_data,
        join_columns=["CustId"],
        track_columns=["Name", "EmailId", "Region"]
    )
    
    # Verify
    result_df = spark.read.format("delta").load(target_path)
    
    # Should have 3 records, all active
    assert result_df.count() == 3
    assert result_df.filter(F.col("IsActive") == True).count() == 3
    
    # All should have StartDate set and EndDate as null
    assert result_df.filter(F.col("StartDate").isNotNull()).count() == 3
    assert result_df.filter(F.col("EndDate").isNull()).count() == 3

def test_scd_type2_update(spark, temp_dir, initial_customer_data, updated_customer_data):
    """Test SCD Type 2 updates with changed data."""
    # Setup
    target_path = os.path.join(temp_dir, "customer_scd")
    handler = SCDType2Handler(spark)
    
    # Initial load
    handler.merge_scd_type2(
        target_path,
        initial_customer_data,
        join_columns=["CustId"],
        track_columns=["Name", "EmailId", "Region"]
    )
    
    # Update with changed data
    handler.merge_scd_type2(
        target_path,
        updated_customer_data,
        join_columns=["CustId"],
        track_columns=["Name", "EmailId", "Region"]
    )
    
    # Verify
    result_df = spark.read.format("delta").load(target_path)
    
    # Should have 5 records total (3 original + 2 updated + 1 new - 1 unchanged)
    assert result_df.count() == 5
    
    # Should have 3 active records
    active_records = result_df.filter(F.col("IsActive") == True)
    assert active_records.count() == 3
    
    # Check C001 history (should have 2 records, 1 active)
    c001_history = result_df.filter(F.col("CustId") == "C001").orderBy("StartDate")
    assert c001_history.count() == 2
    assert c001_history.collect()[0]["IsActive"] == False
    assert c001_history.collect()[0]["Region"] == "North"
    assert c001_history.collect()[0]["EndDate"] is not None
    assert c001_history.collect()[1]["IsActive"] == True
    assert c001_history.collect()[1]["Region"] == "West"
    assert c001_history.collect()[1]["EndDate"] is None
    
    # Check C002 history (should have 2 records, 1 active)
    c002_history = result_df.filter(F.col("CustId") == "C002").orderBy("StartDate")
    assert c002_history.count() == 2
    assert c002_history.collect()[0]["IsActive"] == False
    assert c002_history.collect()[0]["EmailId"] == "jane.smith@example.com"
    assert c002_history.collect()[1]["IsActive"] == True
    assert c002_history.collect()[1]["EmailId"] == "jane.updated@example.com"
    
    # Check C003 (should be unchanged, still active)
    c003_record = result_df.filter(F.col("CustId") == "C003")
    assert c003_record.count() == 1
    assert c