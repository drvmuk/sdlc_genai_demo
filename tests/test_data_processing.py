import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import tempfile
import os
import shutil
from src.data_processing import read_customer_data, read_order_data

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestDataProcessing") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

@pytest.fixture(scope="module")
def sample_data_path():
    # Create a temporary directory for test data
    temp_dir = tempfile.mkdtemp()
    customer_dir = os.path.join(temp_dir, "customer")
    order_dir = os.path.join(temp_dir, "order")
    
    os.makedirs(customer_dir)
    os.makedirs(order_dir)
    
    # Create sample customer data
    with open(os.path.join(customer_dir, "customer.csv"), "w") as f:
        f.write("CustId,Name,EmailId,Region\n")
        f.write("C001,John Doe,john@example.com,North\n")
        f.write("C002,Jane Smith,jane@example.com,South\n")
        f.write("C003,Bob Brown,bob@example.com,East\n")
        f.write("C004,Alice Green,alice@example.com,West\n")
        f.write("C005,,alice@example.com,West\n")  # Invalid record with null name
        f.write("C001,John Doe,john@example.com,North\n")  # Duplicate record
    
    # Create sample order data
    with open(os.path.join(order_dir, "order.csv"), "w") as f:
        f.write("OrderId,ItemName,PricePerUnit,Qty,Date,CustId\n")
        f.write("O001,Laptop,1000.0,2,2023-01-15,C001\n")
        f.write("O002,Mouse,25.5,4,2023-01-16,C002\n")
        f.write("O003,Keyboard,45.75,3,2023-01-17,C003\n")
        f.write("O004,Monitor,150.0,1,2023-01-18,C001\n")
        f.write("O005,Headphones,,2,2023-01-19,C002\n")  # Invalid record with null price
        f.write("O001,Laptop,1000.0,2,2023-01-15,C001\n")  # Duplicate record
    
    yield temp_dir
    
    # Clean up
    shutil.rmtree(temp_dir)

def test_read_customer_data(spark, sample_data_path):
    # Test reading customer data
    customer_path = os.path.join(sample_data_path, "customer")
    df = read_customer_data(spark, customer_path)
    
    # Check that we have the expected number of rows (4 valid records, no duplicates or nulls)
    assert df.count() == 4
    
    # Check schema
    assert df.schema.names == ["CustId", "Name", "EmailId", "Region"]
    
    # Check data
    data = df.collect()
    assert any(row.CustId == "C001" and row.Name == "John Doe" for row in data)
    assert any(row.CustId == "C002" and row.Name == "Jane Smith" for row in data)
    
    # Check that null values were filtered out
    assert not any(row.CustId == "C005" for row in data)

def test_read_order_data(spark, sample_data_path):
    # Test reading order data
    order_path = os.path.join(sample_data_path, "order")
    df = read_order_data(spark, order_path)
    
    # Check that we have the expected number of rows (4 valid records, no duplicates or nulls)
    assert df.count() == 4
    
    # Check schema
    assert "TotalAmount" in df.schema.names
    
    # Check data
    data = df.collect()
    assert any(row.OrderId == "O001" and row.TotalAmount == 2000.0 for row in data)
    assert any(row.OrderId == "O002" and row.TotalAmount == 102.0 for row in data)
    
    # Check that null values were filtered out
    assert not any(row.OrderId == "O005" for row in data)