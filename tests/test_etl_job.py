import pytest
from pyspark.sql import SparkSession
from etl_job import *

# Create a SparkSession for testing
spark = SparkSession.builder.appName("ETL Job Test").getOrCreate()

# Test data
customer_test_data = spark.createDataFrame([
    ("1", "John Doe", "2022-01-01"),
    ("2", "Jane Doe", "2022-01-15"),
    ("3", "Bob Smith", "2022-02-01")
], ["id", "name", "effective_date"])

order_test_data = spark.createDataFrame([
    ("1", "Order 1", "2022-01-01"),
    ("2", "Order 2", "2022-01-15"),
    ("3", "Order 3", "2022-02-01")
], ["id", "order_name", "effective_date"])

def test_etl_job():
    # Test the ETL job with the test data
    customer_test_data.write.format("csv").save("unity-catalog-volume/customer")
    order_test_data.write.format("csv").save("unity-catalog-volume/order")
    
    etl_job()
    
    # Check the results
    customer_raw_table = DeltaTable.forPath(spark, "bronzezone.data.customer_raw")
    orders_raw_table = DeltaTable.forPath(spark, "bronzezone.data.orders_raw")
    
    assert customer_raw_table.count() == 3
    assert orders_raw_table.count() == 3

    # Clean up
    customer_raw_table.delete()
    orders_raw_table.delete()