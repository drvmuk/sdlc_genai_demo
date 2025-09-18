import pytest
from pyspark.sql import SparkSession
import pandas as pd
from delta.tables import DeltaTable
import os
import tempfile
import shutil

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("TestCustomerOrderProcessing") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

@pytest.fixture(scope="module")
def test_data_path():
    """Create temporary directories for test data"""
    temp_dir = tempfile.mkdtemp()
    customer_path = os.path.join(temp_dir, "customerdata")
    order_path = os.path.join(temp_dir, "orderdata")
    os.makedirs(customer_path, exist_ok=True)
    os.makedirs(order_path, exist_ok=True)
    
    # Create sample customer data
    customer_data = pd.DataFrame({
        'CustId': ['C001', 'C002', 'C003', 'C004', None],
        'Name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Invalid User'],
        'EmailId': ['john@example.com', 'jane@example.com', 'bob@example.com', 'alice@example.com', None],
        'Region': ['East', 'West', 'North', 'South', 'East']
    })
    customer_data.to_csv(os.path.join(customer_path, "customer.csv"), index=False)
    
    # Create sample order data
    order_data = pd.DataFrame({
        'OrderId': ['O001', 'O002', 'O003', 'O004', 'O005', None],
        'ItemName': ['Item1', 'Item2', 'Item3', 'Item4', 'Item5', 'Item6'],
        'PricePerUnit': [10.0, 20.0, 15.0, 25.0, 30.0, 5.0],
        'Qty': [2, 1, 3, 2, 1, None],
        'Date': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-03', '2023-01-02', '2023-01-04'],
        'CustId': ['C001', 'C002', 'C001', 'C003', 'C004', 'C999']
    })
    order_data.to_csv(os.path.join(order_path, "order.csv"), index=False)
    
    yield {"customer_path": customer_path, "order_path": order_path}
    
    # Cleanup
    shutil.rmtree(temp_dir)

def test_data_cleaning(spark, test_data_path):
    """Test data cleaning logic"""
    # Read raw data
    customer_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(test_data_path["customer_path"])
    
    order_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(test_data_path["order_path"])
    
    # Clean customer data
    customer_clean = customer_df.filter(
        (customer_df.CustId.isNotNull()) & 
        (customer_df.Name.isNotNull()) & 
        (customer_df.EmailId.isNotNull()) & 
        (customer_df.Region.isNotNull())
    ).dropDuplicates()
    
    # Clean order data and calculate TotalAmount
    order_clean = order_df.filter(
        (order_df.OrderId.isNotNull()) &
        (order_df.ItemName.isNotNull()) &
        (order_df.PricePerUnit.isNotNull()) &
        (order_df.Qty.isNotNull()) &
        (order_df.Date.isNotNull()) &
        (order_df.CustId.isNotNull())
    ).dropDuplicates() \
     .withColumn("TotalAmount", order_df.PricePerUnit * order_df.Qty)
    
    # Assert customer cleaning worked correctly
    assert customer_clean.count() == 4, "Should have 4 valid customer records"
    
    # Assert order cleaning worked correctly
    assert order_clean.count() == 5, "Should have 5 valid order records"
    
    # Check TotalAmount calculation
    first_order = order_clean.filter(order_clean.OrderId == "O001").first()
    assert first_order.TotalAmount == 20.0, "TotalAmount should be 20.0 for OrderId O001"

def test_join_logic(spark, test_data_path):
    """Test join logic between customer and order data"""
    # Read and clean customer data
    customer_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(test_data_path["customer_path"])
    
    customer_clean = customer_df.filter(
        (customer_df.CustId.isNotNull()) & 
        (customer_df.Name.isNotNull()) & 
        (customer_df.EmailId.isNotNull()) & 
        (customer_df.Region.isNotNull())
    ).dropDuplicates()
    
    # Read and clean order data
    order_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(test_data_path["order_path"])
    
    order_clean = order_df.filter(
        (order_df.OrderId.isNotNull()) &
        (order_df.ItemName.isNotNull()) &
        (order_df.PricePerUnit.isNotNull()) &
        (order_df.Qty.isNotNull()) &
        (order_df.Date.isNotNull()) &
        (order_df.CustId.isNotNull())
    ).dropDuplicates() \
     .withColumn("TotalAmount", order_df.PricePerUnit * order_df.Qty)
    
    # Join data
    joined_data = customer_clean.join(
        order_clean,
        on="CustId",
        how="inner"
    ).select(
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
    
    # Verify join results
    assert joined_data.count() == 5, "Should have 5 records after joining"
    
    # Check that customer C001 has 2 orders
    c001_orders = joined_data.filter(joined_data.CustId == "C001").count()
    assert c001_orders == 2, "Customer C001 should have 2 orders"

def test_aggregation_logic(spark, test_data_path):
    """Test aggregation logic for customeraggregatespend"""
    # Read and clean customer data
    customer_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(test_data_path["customer_path"])
    
    customer_clean = customer_df.filter(
        (customer_df.CustId.isNotNull()) & 
        (customer_df.Name.isNotNull()) & 
        (customer_df.EmailId.isNotNull()) & 
        (customer_df.Region.isNotNull())
    ).dropDuplicates()
    
    # Read and clean order data
    order_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(test_data_path["order_path"])
    
    order_clean = order_df.filter(
        (order_df.OrderId.isNotNull()) &
        (order_df.ItemName.isNotNull()) &
        (order_df.PricePerUnit.isNotNull()) &
        (order_df.Qty.isNotNull()) &
        (order_df.Date.isNotNull()) &
        (order_df.CustId.isNotNull())
    ).dropDuplicates() \
     .withColumn("TotalAmount", order_df.PricePerUnit * order_df.Qty)
    
    # Join data
    joined_data = customer_clean.join(
        order_clean,
        on="CustId",
        how="inner"
    ).select(
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
    
    # Aggregate data
    aggregated_data = joined_data.groupBy("Name", "Date").agg({"TotalAmount": "sum"}) \
        .withColumnRenamed("sum(TotalAmount)", "TotalAmount")
    
    # Verify aggregation results
    assert aggregated_data.count() == 5, "Should have 5 records after aggregation"
    
    # Check John Doe's total amount for 2023-01-01
    john_total = aggregated_data.filter(
        (aggregated_data.Name == "John Doe") & 
        (aggregated_data.Date == "2023-01-01")
    ).first()
    
    assert john_total.TotalAmount == 20.0, "John Doe's total for 2023-01-01 should be 20.0"