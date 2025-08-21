import pytest
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

# Import the functions to test
# Note: In a real environment, you'd import the functions from src.dlt_pipeline
# However, for testing DLT pipelines, we'll need to mock the dlt functionality

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("Test Customer Order Pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )

@pytest.fixture
def sample_customer_data(spark):
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
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", None, "invalid@example.com", "North"),  # Invalid record with null
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate record
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_order_data(spark):
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
        ("O002", "Phone", 800.0, 2, datetime.date(2023, 1, 20), "C002"),
        ("O003", "Tablet", 500.0, 1, datetime.date(2023, 1, 25), "C003"),
        ("O004", "Monitor", 300.0, 2, datetime.date(2023, 1, 30), "C001"),
        ("O005", "Keyboard", 100.0, None, datetime.date(2023, 2, 5), "C004"),  # Invalid record with null
        ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001")  # Duplicate record
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_ordersummary_data(spark):
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("TotalAmount", DoubleType(), True),
        StructField("IsActive", BooleanType(), True),
        StructField("StartDate", TimestampType(), True),
        StructField("EndDate", TimestampType(), True)
    ])
    
    current_timestamp = datetime.datetime.now()
    
    data = [
        ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1200.0, 1, 
         datetime.date(2023, 1, 15), 1200.0, True, current_timestamp, None),
        ("C002", "Jane Smith", "jane@example.com", "South", "O002", "Phone", 800.0, 2, 
         datetime.date(2023, 1, 20), 1600.0, True, current_timestamp, None),
        ("C003", "Bob Johnson", "bob@example.com", "East", "O003", "Tablet", 500.0, 1, 
         datetime.date(2023, 1, 25), 500.0, True, current_timestamp, None),
        ("C001", "John Doe", "john@example.com", "North", "O004", "Monitor", 300.0, 2, 
         datetime.date(2023, 1, 30), 600.0, True, current_timestamp, None)
    ]
    
    return spark.createDataFrame(data, schema)

def test_clean_customer_data(spark, sample_customer_data):
    # Test cleaning of customer data (removing nulls and duplicates)
    from pyspark.sql.functions import col
    
    # Apply the same transformation as in the pipeline
    cleaned_data = (
        sample_customer_data
        .filter(
            col("CustId").isNotNull() &
            col("Name").isNotNull() &
            col("EmailId").isNotNull() &
            col("Region").isNotNull()
        )
        .dropDuplicates(["CustId"])
    )
    
    # Verify results
    assert cleaned_data.count() == 4  # Should have 4 valid unique records
    assert cleaned_data.filter(col("Name").isNull()).count() == 0  # No nulls
    
    # Check specific customer exists
    john_record = cleaned_data.filter(col("CustId") == "C001").first()
    assert john_record is not None
    assert john_record["Name"] == "John Doe"

def test_clean_order_data_with_total(spark, sample_order_data):
    # Test cleaning of order data and adding TotalAmount
    from pyspark.sql.functions import col
    
    # Apply the same transformation as in the pipeline
    cleaned_data = (
        sample_order_data
        .filter(
            col("OrderId").isNotNull() &
            col("ItemName").isNotNull() &
            col("PricePerUnit").isNotNull() &
            col("Qty").isNotNull() &
            col("Date").isNotNull() &
            col("CustId").isNotNull()
        )
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        .dropDuplicates(["OrderId"])
    )
    
    # Verify results
    assert cleaned_data.count() == 4  # Should have 4 valid unique records
    assert cleaned_data.filter(col("Qty").isNull()).count() == 0  # No nulls
    
    # Check TotalAmount calculation
    laptop_order = cleaned_data.filter(col("OrderId") == "O001").first()
    assert laptop_order is not None
    assert laptop_order["TotalAmount"] == 1200.0  # 1200 * 1
    
    phone_order = cleaned_data.filter(col("OrderId") == "O002").first()
    assert phone_order is not None
    assert phone_order["TotalAmount"] == 1600.0  # 800 * 2

def test_join_customer_order(spark, sample_customer_data, sample_order_data):
    # Test joining customer and order data
    from pyspark.sql.functions import col
    
    # Clean customer data
    clean_customer = (
        sample_customer_data
        .filter(
            col("CustId").isNotNull() &
            col("Name").isNotNull() &
            col("EmailId").isNotNull() &
            col("Region").isNotNull()
        )
        .dropDuplicates(["CustId"])
    )
    
    # Clean order data
    clean_order = (
        sample_order_data
        .filter(
            col("OrderId").isNotNull() &
            col("ItemName").isNotNull() &
            col("PricePerUnit").isNotNull() &
            col("Qty").isNotNull() &
            col("Date").isNotNull() &
            col("CustId").isNotNull()
        )
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        .dropDuplicates(["OrderId"])
    )
    
    # Join data
    joined_data = (
        clean_customer
        .join(
            clean_order,
            "CustId",
            "inner"
        )
        .select(
            "CustId", 
            "Name", 
            "EmailId", 
            "Region", 
            "OrderId", 
            "ItemName", 
            "PricePerUnit", 
            "Qty", 
            "Date",
            "TotalAmount"
        )
    )
    
    # Verify results
    assert joined_data.count() == 4  # Should have 4 joined records
    
    # Check specific joined record
    john_order = joined_data.filter((col("CustId") == "C001") & (col("OrderId") == "O001")).first()
    assert john_order is not None
    assert john_order["Name"] == "John Doe"
    assert john_order["ItemName"] == "Laptop"
    assert john_order["TotalAmount"] == 1200.0

def test_aggregate_customer_spend(spark, sample_ordersummary_data):
    # Test aggregating customer spend
    from pyspark.sql.functions import col, sum as sum_
    
    # Aggregate data
    aggregated_data = (
        sample_ordersummary_data
        .filter(col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg(sum_("TotalAmount").alias("TotalAmount"))
    )
    
    # Verify results
    assert aggregated_data.count() == 3  # Should have 3 aggregated records (John has 2 orders on different dates)
    
    # Check specific aggregated record
    john_spend_jan15 = aggregated_data.filter((col("Name") == "John Doe") & (col("Date") == datetime.date(2023, 1, 15))).first()
    assert john_spend_jan15 is not None
    assert john_spend_jan15["TotalAmount"] == 1200.0
    
    john_spend_jan30 = aggregated_data.filter((col("Name") == "John Doe") & (col("Date") == datetime.date(2023, 1, 30))).first()
    assert john_spend_jan30 is not None
    assert john_spend_jan30["TotalAmount"] == 600.0