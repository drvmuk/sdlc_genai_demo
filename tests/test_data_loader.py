import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import tempfile
from src.data_loader import DataLoader

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("DataLoaderTest") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

@pytest.fixture(scope="module")
def sample_customer_data(spark):
    """Create sample customer data for testing."""
    schema = StructType([
        StructField("CustId", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    data = [
        (1, "John Doe", "john.doe@example.com", "North"),
        (2, "Jane Smith", "jane.smith@example.com", "South"),
        (3, "Bob Johnson", "bob.johnson@example.com", "East")
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def sample_order_data(spark):
    """Create sample order data for testing."""
    schema = StructType([
        StructField("OrderId", IntegerType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", IntegerType(), True)
    ])
    
    data = [
        (101, "Item A", 10.5, 2, "2023-01-15", 1),
        (102, "Item B", 20.0, 1, "2023-01-16", 2),
        (103, "Item C", 15.75, 3, "2023-01-17", 3)
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def temp_dir():
    """Create a temporary directory for test data."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup is handled by the OS

def test_validate_schema(spark, sample_customer_data):
    """Test schema validation functionality."""
    data_loader = DataLoader(spark)
    
    # Test