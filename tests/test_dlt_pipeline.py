import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
from pyspark.sql import functions as F

# Import the functions to test
# Note: In a real environment, you would import from src.dlt_pipeline
# but for testing purposes we'll redefine the necessary functions

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder
        .appName("CustomerOrderProcessingTest")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
        ("C001", "John Doe", "john.doe@example.com", "North")  # Duplicate
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
        ("O002", "Mouse", 25.0, 2, datetime.date(2023, 1, 20), "C002"),
        ("O003", "Keyboard", 50.0, 1, datetime.date(2023, 1, 25), "C003"),
        ("O004", "Monitor", 300.0, 2, datetime.date(2023, 1, 30), "C001"),
        ("O005", "Headphones", 100.0, None, datetime.date(2023, 2, 5), "C002"),  # Null quantity
        ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001")  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

def test_clean_customer_data(spark, sample_customer_data):
    """Test cleaning of customer data (removing nulls and duplicates)."""
    # Register the DataFrame as a temporary view
    sample_customer_data.createOrReplaceTempView("customer_bronze")
    
    # Apply the transformations
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
    assert cleaned_df.count() == 4  # Should have 4 valid unique customers
    assert "C005" not in [row.CustId for row in cleaned_df.collect()]  # Null name record should be removed

def test_clean_order_data_with_total_amount(spark, sample_order_data):
    """Test cleaning of order data and calculation of TotalAmount."""
    # Register the DataFrame as a temporary view
    sample_order_data.createOrReplaceTempView("order_bronze")
    
    # Apply the transformations
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
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .dropDuplicates(["OrderId"])
    )
    
    # Verify the results
    assert cleaned_df.count() == 4  # Should have 4 valid unique orders
    assert "O005" not in [row.OrderId for row in cleaned_df.collect()]  # Null qty record should be removed
    
    # Check TotalAmount calculation
    order_o002 = cleaned_df.filter(F.col("OrderId") == "O002").first()
    assert order_o002.TotalAmount == 50.0  # 25.0 * 2 = 50.0

def test_join_customer_and_order(spark, sample_customer_data, sample_order_data):
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
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .dropDuplicates(["OrderId"])
    )
    
    # Join the data
    joined_df = (
        clean_customer
        .join(
            clean_order,
            "CustId",
            "inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
        )
    )
    
    # Verify the results
    assert joined_df.count() == 4  # Should have 4 valid joined records
    
    # Check that the join worked correctly
    john_orders = joined_df.filter(F.col("Name") == "John Doe").count()
    assert john_orders == 2  # John Doe has 2 orders (O001 and O004)

def test_customer_aggregate_spend(spark, sample_customer_data, sample_order_data):
    """Test aggregation of customer spending."""
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
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .dropDuplicates(["OrderId"])
    )
    
    joined_df = (
        clean_customer
        .join(
            clean_order,
            "CustId",
            "inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
        )
        .withColumn("IsActive", F.lit(True))
    )
    
    # Aggregate the data
    agg_df = (
        joined_df
        .filter(F.col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )
    
    # Verify the results
    assert agg_df.count() == 4  # Should have 4 aggregated records (one per customer-date combination)
    
    # Check specific aggregation
    john_jan_15 = agg_df.filter((F.col("Name") == "John Doe") & (F.col("Date") == datetime.date(2023, 1, 15))).first()
    assert john_jan_15.TotalAmount == 1200.0  # John's Jan 15 order was $1200