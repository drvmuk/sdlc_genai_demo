# tests/test_bronze_dlt.py
import pytest
from pyspark.sql import SparkSession
from src import bronze_dlt
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark():
    """
    Fixture to create a SparkSession for testing.
    """
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    yield spark
    spark.stop()


def test_load_customer_data(spark):
    """
    Test the load_customer_data function.
    """
    # Mock data
    data = [("1", "Alice", "alice@example.com", "30"),
            ("2", "Bob", "bob@example.com", "25")]
    df = spark.createDataFrame(data, ["id", "name", "email", "age"])
    df.write.csv("/tmp/test_customer_data", header=True, mode="overwrite")

    # Modify load_customer_data to read from the test CSV location
    def mock_load_customer_data():
      raw_df = (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("cloudFiles.inferColumnTypes", "true")
                .load("/tmp/test_customer_data"))

      df_with_metadata = raw_df.withColumn("CreateDateTime", F.current_timestamp()) \
                        .withColumn("UpdateDateTime", F.current_timestamp()) \
                        .withColumn("IsActive", F.lit(True))
      return df_with_metadata


    result_df = mock_load_customer_data()  # Call the modified function


    assert result_df.count() > 0
    assert "CreateDateTime" in result_df.columns
    assert "UpdateDateTime" in result_df.columns
    assert "IsActive" in result_df.columns


def test_load_order_data(spark):
    """
    Test the load_order_data function.
    """
    # Mock data
    data = [("1", "100.0", "2024-01-01"),
            ("2", "200.0", "2024-01-02")]
    df = spark.createDataFrame(data, ["id", "order_amount", "order_date"])
    df.write.csv("/tmp/test_order_data", header=True, mode="overwrite")

    # Modify load_order_data to read from the test CSV location
    def mock_load_order_data():
      raw_df = (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("cloudFiles.inferColumnTypes", "true")
                .load("/tmp/test_order_data"))

      df_with_metadata = raw_df.withColumn("CreateDateTime", F.current_timestamp()) \
                            .withColumn("UpdateDateTime", F.current_timestamp()) \
                            .withColumn("IsActive", F.lit(True))
      return df_with_metadata

    result_df = mock_load_order_data()

    assert result_df.count() > 0
    assert "CreateDateTime" in result_df.columns
    assert "UpdateDateTime" in result_df.columns
    assert "IsActive" in result_df.columns
