import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.batch_processing import clean_data

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("Test Customer Order Processing") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def test_clean_data(spark):
    """Test the clean_data function."""
    # Create sample customer data with nulls and duplicates
    customer_data = [
        (1, "John Doe", "john@example.com", "North"),
        (1, "John Doe", "john@example.com", "North"),  # Duplicate
        (2, None, "jane@example.com", "South"),        # Null name
        (3, "Bob Smith", "bob@example.com", "East")
    ]
    customer_schema = ["CustId", "Name", "EmailId", "Region"]
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Create sample order data with nulls and duplicates
    order_data = [
        (101, "Item1", 10.0, 2, "2023-01-01", 1),
        (101, "Item1", 10.0, 2, "2023-01-01", 1),  # Duplicate
        (102, "Item2", None, 3, "2023-01-02", 2),  # Null price
        (103, "Item3", 15.0, 1, "2023-01-03", 3)
    ]
    order_schema = ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
    order_df = spark.createDataFrame(order_data, order_schema)
    
    # Clean the data
    clean_customer_df, clean_order_df = clean_data(customer_df, order_df)
    
    # Verify customer data cleaning
    assert clean_customer_df.count() == 2  # Should remove duplicates and nulls
    
    # Verify order data cleaning
    assert clean_order_df.count() == 2  # Should remove duplicates and nulls
    
    # Verify TotalAmount calculation
    total_amount = clean_order_df.filter(F.col("OrderId") == 101).select("TotalAmount").collect()[0][0]
    assert total_amount == 20.0  # 10.0 * 2 = 20.0
    
    # Verify TotalAmount calculation for another record
    total_amount = clean_order_df.filter(F.col("OrderId") == 103).select("TotalAmount").collect()[0][0]
    assert total_amount == 15.0  # 15.0 * 1 = 15.0