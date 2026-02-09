"""
Tests for the transformer module.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
import tempfile
from datetime import datetime

from transaction_analytics.src.transformer import (
    clean_transaction_data, enrich_transaction_data,
    calculate_customer_metrics, calculate_category_metrics,
    calculate_regional_metrics, identify_top_customers
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return (SparkSession.builder
            .master("local[2]")
            .appName("TestTransformer")
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
            .getOrCreate())


@pytest.fixture(scope="module")
def raw_transaction_data(spark):
    """Create raw transaction data with issues for testing."""
    data = [
        ("t1", "c1", datetime.now(), 100.0, "Groceries", "s1", "Credit", "true"),
        ("t2", "c2", datetime.now(), 50.0, "Electronics", "s2", "Debit", "1"),
        ("t3", "c1", datetime.now(), 75.0, "Food & Dining", "s3", "Cash", "false"),
        ("t4", "c3", datetime.now(), 200.0, None, "s1", None, "no"),
        ("t5", "c2", datetime.now(), 150.0, "Clothing", "s4", "Credit", "yes")
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
def customer_data(spark):
    """Create customer data for testing."""
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
def store_data(spark):
    """Create store data for testing."""
    data = [
        ("s1", "Store 1", "New York", "East", "Flagship"),
        ("s2", "Store 2", "Los Angeles", "West", "Standard"),
        ("s3", "Store 3", "Chicago", "Midwest", "Express"),
        ("s4", "Store 4", "Miami", "South", "Standard")
    ]
    
    schema = StructType([
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("region", StringType(), True),
        StructField("store_type", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


def test_clean_transaction_data(spark, raw_transaction_data):
    """Test cleaning transaction data."""
    cleaned_df = clean_transaction_data(raw_transaction_data)
    
    # Check that is_online was converted to boolean
    assert cleaned_df.schema["is_online"].dataType == BooleanType()
    
    # Check that missing categories were filled
    assert cleaned_df.filter("category = 'Uncategorized'").count() == 1
    
    # Check that category names were standardized
    assert cleaned_df.filter("category = 'Grocery'").count() == 1
    assert cleaned_df.filter("category = 'Food'").count() == 1
    
    # Check that missing payment methods were filled
    assert cleaned_df.filter("payment_method = 'Unknown'").count() == 1


def test_enrich_transaction_data(spark, raw_transaction_data, customer_data, store_data):
    """Test enriching transaction data."""
    cleaned_df = clean_transaction_data(raw_transaction_data)
    enriched_df = enrich_transaction_data(cleaned_df, customer_data, store_data)
    
    # Check that customer data was joined
    assert "customer_segment" in enriched_df.columns
    assert "loyalty_tier" in enriched_df.columns
    
    # Check that store data was joined
    assert "store_type" in enriched_df.columns
    assert "region" in enriched_df.columns
    
    # Check that date dimensions were added
    assert "transaction_year" in enriched_df.columns
    assert "transaction_month" in enriched_df.columns
    assert "transaction_day" in enriched_df.columns
    assert "transaction_dow" in enriched_df.columns


def test_calculate_customer_metrics(spark, raw_transaction_data, customer_data, store_data):
    """Test calculating customer metrics."""
    cleaned_df = clean_transaction_data(raw_transaction_data)
    enriched_df = enrich_transaction_data(cleaned_df, customer_data, store_data)
    customer_metrics = calculate_customer_metrics(enriched_df)
    
    # Check that metrics were calculated
    assert "total_transactions" in customer_metrics.columns
    assert "total_spend" in customer_metrics.columns
    assert "avg_transaction_value" in customer_metrics.columns
    assert "online_spend_ratio" in customer_metrics.columns
    
    # Check that metrics are correct
    c1_row = customer_metrics.filter("customer_id = 'c1'").collect()[0]
    assert c1_row.total_transactions == 2
    assert c1_row.total_spend == 175.0  # 100 + 75


def test_calculate_category