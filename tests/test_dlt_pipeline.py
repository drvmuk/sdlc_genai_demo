import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("DLT Pipeline Tests") \
        .master("local[1]") \
        .getOrCreate()

def test_order_schema():
    """Test the order schema definition."""
    from src.dlt_pipeline import order_schema
    
    # Verify schema has the expected fields
    field_names = [field.name for field in order_schema.fields]
    assert "OrderId" in field_names
    assert "ItemName" in field_names
    assert "PricePerUnit" in field_names
    assert "Qty" in field_names
    assert "Date" in field_names
    assert "CustId" in field_names

def test_customer_schema():
    """Test the customer schema definition."""
    from src.dlt_pipeline import customer_schema
    
    # Verify schema has the expected fields
    field_names = [field.name for field in customer_schema.fields]
    assert "CustId" in field_names
    assert "Name" in field_names
    assert "EmailId" in field_names
    assert "Region" in field_names