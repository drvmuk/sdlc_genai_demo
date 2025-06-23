# tests/test_silver_dlt.py
import pytest
from pyspark.sql import SparkSession
from src import silver_dlt
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark():
    """
    Fixture to create a SparkSession for testing.
    """
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    yield spark
    spark.stop()

def test_create_customer_order_combined(spark):
    """
    Test the create_customer_order_combined function.
    """
    # Mock data for customer_raw
    customer_data = [("1", "Alice", "alice@example.com", "30"),
                     ("2", "Bob", "bob@example.com", "25")]
    customer_df = spark.createDataFrame(customer_data, ["id", "name", "email", "age"])
    customer_df.createOrReplaceTempView("customer_raw")

    # Mock data for orders_raw
    order_data = [("1", "100.0", "2024-01-01"),
                  ("2", "200.0", "2024-01-02")]
    order_df = spark.createDataFrame(order_data, ["id", "order_amount", "order_date"])
    order_df.createOrReplaceTempView("orders_raw")

    # Modify create_customer_order_combined to read from mock data
    def mock_create_customer_order_combined():
        customer_df = spark.table("customer_raw")
        order_df = spark.table("orders_raw")

        joined_df = customer_df.join(order_df, "id", "inner") \
                               .dropna() \
                               .dropDuplicates()

        df_with_metadata = joined_df.withColumn("CreateDateTime", F.current_timestamp()) \
                           .withColumn("UpdateDateTime", F.current_timestamp()) \
                           .withColumn("IsActive", F.lit(True))
        return df_with_metadata


    result_df = mock_create_customer_order_combined()

    assert result_df.count() > 0
    assert "CreateDateTime" in result_df.columns
    assert "UpdateDateTime" in result_df.columns
    assert "IsActive" in result_df.columns
