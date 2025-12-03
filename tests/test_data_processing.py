import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import os
import tempfile
from datetime import date

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing"""
    return SparkSession.builder \
        .appName("TestCustomerOrderProcessing") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing"""
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        (None, "Invalid Customer", "invalid@example.com", "West"),  # Null CustId
        ("C004", None, "no_name@example.com", "North"),  # Null Name
        ("C001", "John Doe", "john@example.com", "North"),  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing"""
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    data = [
        ("O001", "Item1", 10.0, 2, date(2023, 1, 15), "C001"),
        ("O002", "Item2", 15.0, 1, date(2023, 1, 20), "C002"),
        ("O003", "Item3", 20.0, 3, date(2023, 1, 25), "C003"),
        ("O004", "Item4", 25.0, 2, date(2023, 1, 30), "C001"),
        (None, "Invalid Order", 5.0, 1, date(2023, 2, 1), "C002"),  # Null OrderId
        ("O005", "Item5", None, 2, date(2023, 2, 5), "C003"),  # Null Price
        ("O001", "Item1", 10.0, 2, date(2023, 1, 15), "C001"),  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

def test_data_cleaning(spark, sample_customer_data, sample_order_data):
    """Test data cleaning logic"""
    # Clean customer data
    clean_customer = (
        sample_customer_data
        .dropDuplicates()
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
    )
    
    # Clean order data and add TotalAmount
    clean_order = (
        sample_order_data
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .dropDuplicates()
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
    )
    
    # Verify customer data cleaning
    assert clean_customer.count() == 3  # 3 valid unique customers
    
    # Verify order data cleaning and TotalAmount calculation
    assert clean_order.count() == 4  # 4 valid unique orders
    
    # Check TotalAmount calculation
    order_with_total = clean_order.filter(F.col("OrderId") == "O001").first()
    assert order_with_total["TotalAmount"] == 20.0  # 10.0 * 2 = 20.0

def test_join_and_aggregation(spark, sample_customer_data, sample_order_data):
    """Test join and aggregation logic"""
    # Clean data first
    clean_customer = (
        sample_customer_data
        .dropDuplicates()
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
    )
    
    clean_order = (
        sample_order_data
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .dropDuplicates()
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
    )
    
    # Join customer and order
    joined_df = (
        clean_order
        .join(
            clean_customer,
            on="CustId",
            how="inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
        )
    )
    
    # Aggregate by customer and date
    agg_df = (
        joined_df
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )
    
    # Verify join results
    assert joined_df.count() == 4  # All valid orders should join with customers
    
    # Verify aggregation results
    john_orders = agg_df.filter(F.col("Name") == "John Doe").collect()
    assert len(john_orders) == 2  # John has orders on 2 different dates
    
    # Find John's total for Jan 15
    john_jan15 = next((row for row in john_orders if row["Date"] == date(2023, 1, 15)), None)
    assert john_jan15 is not None
    assert john_jan15["TotalAmount"] == 20.0  # 10.0 * 2 = 20.0