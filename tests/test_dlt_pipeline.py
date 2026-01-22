import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import tempfile
import os
import sys
from unittest.mock import patch, MagicMock

# Add the src directory to the path so we can import the module
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# Mock the dlt module
class MockDLT:
    def table(self, name=None, comment=None, table_properties=None):
        def decorator(func):
            return func
        return decorator
    
    def expect_or_fail(self, name, condition):
        def decorator(func):
            return func
        return decorator
    
    def read(self, table_name):
        # This would normally return a DataFrame from the DLT pipeline
        # For testing, we'll just return a mock based on the table name
        if table_name == "customer":
            return self.mock_customer_df
        elif table_name == "order":
            return self.mock_order_df
        elif table_name == "ordersummary":
            return self.mock_ordersummary_df
        else:
            raise ValueError(f"Unknown table: {table_name}")

# Create a mock dlt module
mock_dlt = MockDLT()

# Patch the dlt module
@pytest.fixture(scope="module")
def patch_dlt():
    with patch.dict("sys.modules", {"dlt": mock_dlt}):
        yield mock_dlt

@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("TestDLTPipeline") \
        .master("local[1]") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

@pytest.fixture(scope="module")
def setup_mock_data(spark, patch_dlt):
    """Set up mock data for the DLT pipeline."""
    # Create sample customer data
    customer_schema = StructType([
        StructField("CustId", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    customer_data = [
        (1, "John Doe", "john@example.com", "North"),
        (2, "Jane Smith", "jane@example.com", "South"),
        (3, "Bob Johnson", "bob@example.com", "East"),
        (4, "Alice Brown", "alice@example.com", "West")
    ]
    
    patch_dlt.mock_customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Create sample order data
    order_schema = StructType([
        StructField("OrderId", IntegerType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", IntegerType(), True),
        StructField("TotalAmount", DoubleType(), True)
    ])
    
    order_data = [
        (101, "Item A", 10.0, 2, datetime.date(2023, 1, 15), 1, 20.0),
        (102, "Item B", 15.0, 1, datetime.date(2023, 1, 16), 2, 15.0),
        (103, "Item C", 20.0, 3, datetime.date(2023, 1, 17), 3, 60.0),
        (104, "Item D", 5.0, 4, datetime.date(2023, 1, 18), 4, 20.0)
    ]
    
    patch_dlt.mock_order_df = spark.createDataFrame(order_data, order_schema)
    
    # Create sample ordersummary data
    ordersummary_schema = StructType([
        StructField("CustId", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OrderId", IntegerType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("IsActive", StringType(), True),
        StructField("StartDate", DateType(), True),
        StructField("EndDate", DateType(), True),
        StructField("TotalAmount", DoubleType(), True)
    ])
    
    ordersummary_data = [
        (1, "John Doe", "john@example.com", "North", 101, "Item A", 10.0, 2, 
         datetime.date(2023, 1, 15), "true", datetime.date(2023, 1, 1), None, 20.0),
        (2, "Jane Smith", "jane@example.com", "South", 102, "Item B", 15.0, 1, 
         datetime.date(2023, 1, 16), "true", datetime.date(2023, 1, 1), None, 15.0),
        (3, "Bob Johnson", "bob@example.com", "East", 103, "Item C", 20.0, 3, 
         datetime.date(2023, 1, 17), "true", datetime.date(2023, 1, 1), None, 60.0),
        (4, "Alice Brown", "alice@example.com", "West", 104, "Item D", 5.0, 4, 
         datetime.date(2023, 1, 18), "true", datetime.date(2023, 1, 1), None, 20.0)
    ]
    
    patch_dlt.mock_ordersummary_df = spark.createDataFrame(ordersummary_data, ordersummary_schema)

def test_customer_table(spark, patch_dlt, setup_mock_data):
    """Test the customer table function."""
    # Import the function to test
    from src.dlt_pipeline import customer
    
    # Set the global spark variable
    global spark
    spark = spark
    
    # Call the function
    with patch('src.dlt_pipeline.spark', spark):
        result_df = customer()
    
    # Verify the result
    assert result_df.count() == 4
    assert "CustId" in result_df.columns
    assert "Name" in result_df.columns
    assert "EmailId" in result_df.columns
    assert "Region" in result_df.columns

def test_order_table(spark, patch_dlt, setup_mock_data):
    """Test the order table function."""
    # Import the function to test
    from src.dlt_pipeline import order
    
    # Set the global spark variable
    global spark
    spark = spark
    
    # Call the function
    with patch('src.dlt_pipeline.spark', spark):
        result_df = order()
    
    # Verify the result
    assert result_df.count() == 4
    assert "OrderId" in result_df.columns
    assert "ItemName" in result_df.columns
    assert "PricePerUnit" in result_df.columns
    assert "Qty" in result_df.columns
    assert "Date" in result_df.columns
    assert "CustId" in result_df.columns
    assert "TotalAmount" in result_df.columns
    
    # Check TotalAmount calculation
    row = result_df.filter("OrderId = 101").first()
    assert row["TotalAmount"] == row["PricePerUnit"] * row["Qty"]

def test_customeraggregatespend_table(spark, patch_dlt, setup_mock_data):
    """Test the customeraggregatespend table function."""
    # Import the function to test
    from src.dlt_pipeline import customeraggregatespend
    
    # Call the function
    result_df = customeraggregatespend()
    
    # Verify the result
    assert result_df.count() == 4  # One