"""
Unit tests for customer order pipeline.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
from src.utils import validate_data, apply_scd_type2_changes

@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing.
    """
    return SparkSession.builder \
        .appName("TestCustomerOrderPipeline") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def customer_schema():
    """
    Define customer schema for tests.
    """
    return StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

@pytest.fixture
def order_schema():
    """
    Define order schema for tests.
    """
    return StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])

@pytest.fixture
def sample_customer_data(spark, customer_schema):
    """
    Create sample customer data for testing.
    """
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", None, "alice@example.com", "West"),  # Contains null
        ("C005", "Tom Brown", "tom@example.com", "North"),
        ("C005", "Tom Brown", "tom@example.com", "North")  # Duplicate
    ]
    
    return spark.createDataFrame(data, customer_schema)

@pytest.fixture
def sample_order_data(spark, order_schema):
    """
    Create sample order data for testing.
    """
    data = [
        ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001"),
        ("O002", "Phone", 800.0, 2, datetime.date(2023, 1, 20), "C002"),
        ("O003", "Tablet", 500.0, 1, datetime.date(2023, 1, 25), "C003"),
        ("O004", "Headphones", 100.0, 3, datetime.date(2023, 1, 30), "C001"),
        ("O005", "Charger", 25.0, 5, datetime.date(2023, 2, 5), "C002"),
        ("O006", "Case", None, 2, datetime.date(2023, 2, 10), "C003"),  # Contains null
        ("O007", "Screen", 50.0, 1, datetime.date(2023, 2, 15), "C005"),
        ("O007", "Screen", 50.0, 1, datetime.date(2023, 2, 15), "C005")  # Duplicate
    ]
    
    return spark.createDataFrame(data, order_schema)

def test_validate_data(spark, sample_customer_data):
    """
    Test data validation function.
    """
    # Test with valid columns
    assert validate_data(sample_customer_data, ["CustId", "Name", "EmailId", "Region"]) == True
    
    # Test with invalid columns
    with pytest.raises(ValueError):
        validate_data(sample_customer_data, ["CustId", "Name", "NonExistentColumn"])

def test_data_cleaning(spark, sample_customer_data, sample_order_data):
    """
    Test data cleaning logic.
    """
    # Clean customer data
    cleaned_customer = sample_customer_data.filter(
        sample_customer_data.CustId.isNotNull() &
        sample_customer_data.Name.isNotNull() &
        sample_customer_data.EmailId.isNotNull() &
        sample_customer_data.Region.isNotNull()
    ).dropDuplicates(["CustId"])
    
    # Verify cleaning results
    assert cleaned_customer.count() == 4  # Should have 4 rows after removing null and duplicate
    
    # Clean order data
    cleaned_order = sample_order_data.filter(
        sample_order_data.OrderId.isNotNull() &
        sample_order_data.ItemName.isNotNull() &
        sample_order_data.PricePerUnit.isNotNull() &
        sample_order_data.Qty.isNotNull() &
        sample_order_data.Date.isNotNull() &
        sample_order_data.CustId.isNotNull()
    ).dropDuplicates(["OrderId"])
    
    # Verify cleaning results
    assert cleaned_order.count() == 5  # Should have 5 rows after removing null and duplicate

def test_total_amount_calculation(spark, sample_order_data):
    """
    Test TotalAmount calculation.
    """
    from pyspark.sql.functions import col
    
    # Calculate TotalAmount
    orders_with_total = sample_order_data.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    # Verify calculation for specific orders
    order_o001 = orders_with_total.filter(col("OrderId") == "O001").first()
    assert order_o001["TotalAmount"] == 1200.0  # 1200 * 1
    
    order_o002 = orders_with_total.filter(col("OrderId") == "O002").first()
    assert order_o002["TotalAmount"] == 1600.0  # 800 * 2

def test_customer_aggregate_spend(spark, sample_customer_data, sample_order_data):
    """
    Test customer aggregate spend calculation.
    """
    from pyspark.sql.functions import col, sum as sum_
    
    # Clean data
    cleaned_customer = sample_customer_data.filter(
        sample_customer_data.CustId.isNotNull() &
        sample_customer_data.Name.isNotNull()
    ).dropDuplicates(["CustId"])
    
    cleaned_order = sample_order_data.filter(
        sample_order_data.OrderId.isNotNull() &
        sample_order_data.PricePerUnit.isNotNull()
    ).withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    # Join and aggregate
    customer_spend = (
        cleaned_order
        .join(cleaned_customer, "CustId", "inner")
        .groupBy("Name", "Date")
        .agg(sum_("TotalAmount").alias("TotalAmount"))
    )
    
    # Verify results
    john_spend = customer_spend.filter(col("Name") == "John Doe").collect()
    assert len(john_spend) == 2  # John has orders on 2 different dates
    
    # Check specific aggregation
    john_jan15 = [row for row in john_spend if row["Date"] == datetime.date(2023, 1, 15)]
    assert len(john_jan15) == 1
    assert john_jan15[0]["TotalAmount"] == 1200.0