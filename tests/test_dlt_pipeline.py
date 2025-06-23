import pytest
from pyspark.sql import SparkSession
from src.dlt_pipeline import load_raw_data, transform_data, load_silver_data, load_gold_data

def test_load_raw_data(spark):
    # Create sample data
    data = [("1", "John", "john@example.com", 25), ("2", "Jane", "jane@example.com", 30)]
    df = spark.createDataFrame(data, ["id", "name", "email", "age"])

    # Load raw data
    raw_df = load_raw_data(spark, "path/to/sample/data")

    # Assert data is loaded correctly
    assert raw_df.count() == 2

def test_transform_data(spark):
    # Create sample data
    data = [("1", "John", "john@example.com", 25), ("2", "Jane", "jane@example.com", 30)]
    df = spark.createDataFrame(data, ["id", "name", "email", "age"])

    # Transform data
    silver_df = transform_data(spark, df)

    # Assert data is transformed correctly
    assert silver_df.count() == 2

def test_load_silver_data(spark):
    # Create sample data
    data = [("1", "John", "john@example.com", 25), ("2", "Jane", "jane@example.com", 30)]
    df = spark.createDataFrame(data, ["id", "name", "email", "age"])

    # Load silver data
    load_silver_data(spark, df)

    # Assert data is loaded correctly
    assert spark.read.table("customer_order_combined").count() == 2

def test_load_gold_data(spark):
    # Create sample data
    data = [("1", "John", "john@example.com", 25), ("2", "Jane", "jane@example.com", 30)]
    df = spark.createDataFrame(data, ["id", "name", "email", "age"])

    # Load gold data
    load_gold_data(spark, df)

    # Assert data is loaded correctly
    assert spark.read.table("customer_order_summary").count() == 2

@pytest.fixture
def spark():
    return SparkSession.builder.appName("Test DLT Pipeline").getOrCreate()