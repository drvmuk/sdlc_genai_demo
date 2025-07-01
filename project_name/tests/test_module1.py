import pytest
from pyspark.sql import SparkSession
from src.module1 import create_bronze_tables, create_silver_table, create_gold_table

@pytest.fixture
def spark():
    return SparkSession.builder.appName("Test ETL Pipeline").getOrCreate()

def test_create_bronze_tables(spark):
    # Create test data
    data = [("1", "John", "john@example.com"), ("2", "Jane", "jane@example.com")]
    df = spark.createDataFrame(data, ["id", "name", "email"])
    df.write.format("csv").mode("overwrite").save("path_to_test_csv")

    create_bronze_tables(spark)

    # Assert bronze tables are created
    assert spark.read.table("bronzezone.data.customer_raw").count() == 2

def test_create_silver_table(spark):
    # Create test data
    customer_data = [("1", "John", "john@example.com"), ("2", "Jane", "jane@example.com")]
    orders_data = [("1", 100.0), ("2", 200.0)]
    customer_df = spark.createDataFrame(customer_data, ["id", "name", "email"])
    orders_df = spark.createDataFrame(orders_data, ["id", "order_amount"])
    customer_df.write.format("delta").mode("overwrite").saveAsTable("bronzezone.data.customer_raw")
    orders_df.write.format("delta").mode("overwrite").saveAsTable("bronzezone.data.orders_raw")

    create_silver_table(spark)

    # Assert silver table is created
    assert spark.read.table("silverzone.data.customer_order_combined").count() == 2

def test_create_gold_table(spark):
    # Create test data
    data = [("1", "John", "john@example.com", 100.0), ("2", "Jane", "jane@example.com", 200.0)]
    df = spark.createDataFrame(data, ["id", "name", "email", "order_amount"])
    df.write.format("delta").mode("overwrite").saveAsTable("silverzone.data.customer_order_combined")

    create_gold_table(spark)

    # Assert gold table is created
    assert spark.read.table("goldzone.data.customer_order_summary").count() == 2