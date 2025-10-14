import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return (
        SparkSession.builder
        .appName("CustomerOrderDLTTests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )

@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing."""
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", "Jane Smith", "jane.smith@example.com", "South"),
        ("C003", "Bob Johnson", "bob.johnson@example.com", "East"),
        ("C004", "Alice Brown", "alice.brown@example.com", "West"),
        ("C005", None, "invalid@example.com", "North"),  # Null name
        ("C003", "Bob Johnson", "bob.johnson@example.com", "East"),  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing."""
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    data = [
        ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001"),
        ("O002", "Mouse", 25.0, 2, datetime.date(2023, 1, 16), "C002"),
        ("O003", "Keyboard", 45.0, 1, datetime.date(2023, 1, 17), "C003"),
        ("O004", "Monitor", 200.0, 2, datetime.date(2023, 1, 18), "C004"),
        ("O005", "Headphones", 150.0, 1, datetime.date(2023, 1, 19), "C001"),
        ("O006", None, 50.0, 3, datetime.date(2023, 1, 20), "C002"),  # Null item name
        ("O003", "Keyboard", 45.0, 1, datetime.date(2023, 1, 17), "C003"),  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

def test_clean_customer_data(spark, sample_customer_data):
    """Test cleaning customer data (removing nulls and duplicates)."""
    # Apply the cleaning transformation
    cleaned_df = (
        sample_customer_data
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
        .dropDuplicates(["CustId"])
    )
    
    # Verify the results
    assert cleaned_df.count() == 4  # Should have 4 valid records
    assert cleaned_df.filter(F.col("Name").isNull()).count() == 0  # No nulls
    
    # Check for duplicates
    cust_id_counts = cleaned_df.groupBy("CustId").count()
    assert cust_id_counts.filter(F.col("count") > 1).count() == 0  # No duplicates

def test_clean_order_data(spark, sample_order_data):
    """Test cleaning order data and calculating TotalAmount."""
    # Apply the cleaning transformation and calculate TotalAmount
    cleaned_df = (
        sample_order_data
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
        .dropDuplicates(["OrderId"])
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    )
    
    # Verify the results
    assert cleaned_df.count() == 5  # Should have 5 valid records
    assert cleaned_df.filter(F.col("ItemName").isNull()).count() == 0  # No nulls
    
    # Check for duplicates
    order_id_counts = cleaned_df.groupBy("OrderId").count()
    assert order_id_counts.filter(F.col("count") > 1).count() == 0  # No duplicates
    
    # Check TotalAmount calculation
    total_amount_check = cleaned_df.select(
        "OrderId", "PricePerUnit", "Qty", "TotalAmount",
        (F.col("PricePerUnit") * F.col("Qty")).alias("ExpectedTotal")
    )
    
    # All TotalAmount values should match the expected calculation
    assert total_amount_check.filter(F.col("TotalAmount") != F.col("ExpectedTotal")).count() == 0

def test_join_customer_order(spark, sample_customer_data, sample_order_data):
    """Test joining customer and order data."""
    # Clean the data first
    clean_customer = (
        sample_customer_data
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
        .dropDuplicates(["CustId"])
    )
    
    clean_order = (
        sample_order_data
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
        .dropDuplicates(["OrderId"])
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    )
    
    # Join the data
    joined_df = clean_customer.join(clean_order, "CustId", "inner")
    
    # Verify the results
    assert joined_df.count() == 5  # All valid orders should match with customers
    
    # Check that all required columns are present
    required_columns = [
        "CustId", "Name", "EmailId", "Region", 
        "OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
    ]
    
    for col in required_columns:
        assert col in joined_df.columns

def test_customer_aggregate_spend(spark, sample_customer_data, sample_order_data):
    """Test aggregating customer spending."""
    # Clean and join the data first
    clean_customer = (
        sample_customer_data
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
        .dropDuplicates(["CustId"])
    )
    
    clean_order = (
        sample_order_data
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
        .dropDuplicates(["OrderId"])
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    )
    
    joined_df = clean_customer.join(clean_order, "CustId", "inner")
    
    # Aggregate the data
    agg_df = joined_df.groupBy("Name", "Date").agg(
        F.sum("TotalAmount").alias("TotalAmount")
    )
    
    # Verify the results
    assert agg_df.count() == 5  # Each customer-date combination
    
    # Check John Doe's total spend (should have 2 records on different dates)
    john_spend = agg_df.filter(F.col("Name") == "John Doe").orderBy("Date").collect()
    assert len(john_spend) == 2
    assert john_spend[0]["TotalAmount"] == 1200.0  # Laptop
    assert john_spend[1]["TotalAmount"] == 150.0   # Headphones