import pytest
from pyspark.sql import SparkSession
from src.bronze_layer_ingestion import customer_raw, orders_raw

@pytest.fixture
def spark():
    return SparkSession.builder.appName("test").getOrCreate()

def test_customer_raw(spark):
    # Test customer_raw function
    customer_df = customer_raw()
    assert customer_df.count() > 0

def test_orders_raw(spark):
    # Test orders_raw function
    orders_df = orders_raw()
    assert orders_df.count() > 0
