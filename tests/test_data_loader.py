"""
Tests for the data_loader module.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
import tempfile
from datetime import datetime

from transaction_analytics.src.data_loader import (
    get_spark_session, load_transactions, load_customer_data, load_store_data
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return (SparkSession.builder
            .master("local[2]")
            .appName("TestDataLoader")
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


@pytest.fixture(scope="module")
def sample_transaction_data(spark):
    """Create sample transaction data for testing."""
    data = [
        ("t1", "c1", datetime.now(), 100.0, "Grocery", "s1", "Credit", "false"),
        ("t2", "c2", datetime.now(), 50.0, "Electronics", "s2", "Debit", "true"),
        ("t3", "c1", datetime.now(), 75.0, "Clothing", "s3", "Cash", "false")
    ]
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("transaction_date", TimestampType(), False),
        StructField("amount", DoubleType(), False),
        StructField("category", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("is_online", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def sample_customer_data(spark):
    """Create sample customer data for testing."""
    data = [
        ("c1", "John Doe", "john@example.com", datetime.now(), "Premium", "Gold", "35-44"),
        ("c2", "Jane Smith", "jane@example.com", datetime.now(), "Standard", "Silver", "25-34"),
        ("c3", "Bob Johnson", "bob@example.com", datetime.now(), "Premium", "Bronze", "45-54")
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", TimestampType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("loyalty_tier", StringType(), True),
        StructField("age_group", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def sample_store_data(spark):
    """Create sample store data for testing."""
    data = [
        ("s1", "Store 1", "New York", "East", "Flagship"),
        ("s2", "Store 2", "Los Angeles", "West", "Standard"),
        ("s3", "Store 3", "Chicago", "Midwest", "Express")
    ]
    
    schema = StructType([
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("region", StringType(), True),
        StructField("store_type", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


def test_get_spark_session():
    """Test that get_spark_session returns a valid SparkSession."""
    spark = get_spark_session()
    assert spark is not None
    assert isinstance(spark, SparkSession)


def test_load_transactions_csv(spark, sample_transaction_data, tmpdir):
    """Test loading transactions from CSV."""
    # Create a temporary CSV file with sample data
    csv_path = os.path.join(tmpdir, "transactions")
    sample_transaction_data.write.csv(csv_path, header=True)
    
    # Load the data using the function
    loaded_df = load_transactions(spark, csv_path, "csv")
    
    # Verify the data was loaded correctly
    assert loaded_df.count() == 3
    assert "transaction_id" in loaded_df.columns
    assert "amount" in loaded_df.columns


def test_load_customer_data(spark, sample_customer_data, tmpdir):
    """Test loading customer data."""
    # Create a temporary parquet file with sample data
    parquet_path = os.path.join(tmpdir, "customers")
    sample_customer_data.write.parquet(parquet_path)
    
    # Load the data using the function
    loaded_df = load_customer_data(spark, parquet_path, "parquet")
    
    # Verify the data was loaded correctly
    assert loaded_df.count() == 3
    assert "customer_id" in loaded_df.columns
    assert "loyalty_tier" in loaded_df.columns


def test_load_store_data(spark, sample_store_data, tmpdir):
    """Test loading store data."""
    # Create a temporary delta table with sample data
    delta_path = os.path.join(tmpdir, "stores")
    sample_store_data.write.format("delta").save(delta_path)
    
    # Load the data using the function
    loaded_df = load_store_data(spark, delta_path, "delta")
    
    # Verify the data was loaded correctly
    assert loaded_df.count() == 3
    assert "store_id" in loaded_df.columns
    assert "region" in loaded_df.columns