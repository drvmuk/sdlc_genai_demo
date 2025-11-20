import pytest
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from chispa.dataframe_comparer import assert_df_equality

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestCustomerOrderPipeline") \
        .master("local[2]") \
        .getOrCreate()

def test_calculate_total_amount(spark):
    # Create sample order data
    order_data = [
        ("O001", "Item1", 10.0, 2, datetime.date(2023, 1, 1), "C001"),
        ("O002", "Item2", 15.0, 3, datetime.date(2023, 1, 2), "C002"),
        ("O003", "Item3", 5.0, 5, datetime.date(2023, 1, 3), "C001")
    ]
    
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    order_df = spark.createDataFrame(order_data, order_schema)
    
    # Calculate TotalAmount
    result_df = order_df.withColumn("TotalAmount", order_df["PricePerUnit"] * order_df["Qty"])
    
    # Expected results
    expected_data = [
        ("O001", "Item1", 10.0, 2, datetime.date(2023, 1, 1), "C001", 20.0),
        ("O002", "Item2", 15.0, 3, datetime.date(2023, 1, 2), "C002", 45.0),
        ("O003", "Item3", 5.0, 5, datetime.date(2023, 1, 3), "C001", 25.0)
    ]
    
    expected_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True),
        StructField("TotalAmount", DoubleType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Compare results
    assert_df_equality(result_df, expected_df, ignore_nullable=True)

def test_customer_aggregate_spend(spark):
    # Create sample ordersummary data
    data = [
        ("C001", "John", "john@example.com", "North", "O001", "Item1", 10.0, 2, datetime.date(2023, 1, 1), True, None, None),
        ("C001", "John", "john@example.com", "North", "O002", "Item2", 15.0, 3, datetime.date(2023, 1, 1), True, None, None),
        ("C002", "Jane", "jane@example.com", "South", "O003", "Item3", 5.0, 5, datetime.date(2023, 1, 2), True, None, None)
    ]
    
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
        StructField("IsActive", StringType(), True),
        StructField("StartDate", DateType(), True),
        StructField("EndDate", DateType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Calculate TotalAmount and aggregate
    result_df = df \
        .withColumn("TotalAmount", df["PricePerUnit"] * df["Qty"]) \
        .groupBy("Name", "Date") \
        .agg({"TotalAmount": "sum"}) \
        .withColumnRenamed("sum(TotalAmount)", "TotalAmount") \
        .orderBy("Name", "Date")
    
    # Expected results
    expected_data = [
        ("Jane", datetime.date(2023, 1, 2), 25.0),
        ("John", datetime.date(2023, 1, 1), 65.0)
    ]
    
    expected_schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Date", DateType(), True),
        StructField("TotalAmount", DoubleType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, expected_schema).orderBy("Name", "Date")
    
    # Compare results
    assert_df_equality(result_df, expected_df, ignore_nullable=True)

def test_clean_data_removes_nulls_and_duplicates(spark):
    # Create sample data with nulls and duplicates
    data = [
        ("C001", "John", "john@example.com", "North"),
        ("C002", "Jane", "jane@example.com", "South"),
        ("C002", "Jane", "jane@example.com", "South"),  # Duplicate
        ("C003", None, "bob@example.com", "East"),      # Contains null
        ("C004", "Alice", None, "West"),                # Contains null
        ("C005", "Charlie", "charlie@example.com", "North")
    ]
    
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Clean data
    result_df = df.dropDuplicates().na.drop()
    
    # Expected results
    expected_data = [
        ("C001", "John", "john@example.com", "North"),
        ("C002", "Jane", "jane@example.com", "South"),
        ("C005", "Charlie", "charlie@example.com", "North")
    ]
    
    expected_df = spark.createDataFrame(expected_data, schema)
    
    # Compare results (ignoring order)
    assert result_df.count() == expected_df.count()
    assert set(result_df.collect()) == set(expected_df.collect())