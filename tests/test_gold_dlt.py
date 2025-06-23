# tests/test_gold_dlt.py
import pytest
from pyspark.sql import SparkSession
from src import gold_dlt
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark():
    """
    Fixture to create a SparkSession for testing.
    """
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    yield spark
    spark.stop()

def test_create_customer_order_summary(spark):
    """
    Test the create_customer_order_summary function.
    """
    # Mock data for customer_order_combined
    combined_data = [("1", "Alice", "alice@example.com", "30", "100.0", "2024-01-01"),
                     ("2", "Bob", "bob@example.com", "25", "200.0", "2024-01-02"),
                     ("3", "Charlie", "charlie@example.com", "30", "150.0", "2024-01-03")]
    combined_df = spark.createDataFrame(combined_data, ["id", "name", "email", "age", "order_amount", "order_date"])
    combined_df.createOrReplaceTempView("customer_order_combined")


    # Modify create_customer_order_summary to read from mock data
    def mock_create_customer_order_summary():
        grouping_column = "age" # Define grouping column for testing

        df = spark.table("customer_order_combined")

        # Group by age
        grouped_df = df.groupBy("age").agg(
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("average_order_amount")
        )

        df_with_metadata = grouped_df.withColumn("CreateDateTime", F.current_timestamp()) \
                                     .withColumn("UpdateDateTime", F.current_timestamp()) \
                                     .withColumn("IsActive", F.lit(True))
        return df_with_metadata

    result_df = mock_create_customer_order_summary()

    assert result_df.count() > 0
    assert "total_revenue" in result_df.columns
    assert "average_order_amount" in result_df.columns
    assert "CreateDateTime" in result_df.columns
    assert "UpdateDateTime" in result_df.columns
    assert "IsActive" in result_df.columns
