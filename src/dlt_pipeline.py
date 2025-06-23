from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import *

def load_raw_data(spark, path):
    """Load raw data from Unity Catalog Volumes into Bronze layer."""
    df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(path)
    df = df.withColumn("CreateDateTime", current_timestamp()).withColumn("UpdateDateTime", current_timestamp()).withColumn("IsActive", lit(True))
    return df

def transform_data(spark, bronze_df):
    """Transform data from Bronze layer into Silver layer."""
    # Join Customer and Order data on id column
    silver_df = bronze_df.filter(bronze_df["id"].isNotNull()).dropDuplicates()
    # Apply SCD type 2 to handle data changes
    silver_df = silver_df.withColumn("Version", lit(1))
    return silver_df

def load_silver_data(spark, silver_df):
    """Load transformed data into Silver layer."""
    silver_df.write.format("delta").mode("overwrite").saveAsTable("customer_order_combined")

def load_gold_data(spark, silver_df):
    """Load aggregated data into Gold layer."""
    gold_df = silver_df.groupBy("age").agg({"order_amount": "sum"})
    gold_df.write.format("delta").mode("overwrite").saveAsTable("customer_order_summary")

def main():
    spark = SparkSession.builder.appName("DLT Pipeline").config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

    # Load raw data from Unity Catalog Volumes into Bronze layer
    raw_customer_df = load_raw_data(spark, "path/to/customer/data")
    raw_order_df = load_raw_data(spark, "path/to/order/data")
    raw_customer_df.write.format("delta").mode("overwrite").saveAsTable("customer_raw")
    raw_order_df.write.format("delta").mode("overwrite").saveAsTable("orders_raw")

    # Transform and load data from Bronze layer into Silver layer
    bronze_df = spark.read.table("customer_raw").unionByName(spark.read.table("orders_raw"))
    silver_df = transform_data(spark, bronze_df)
    load_silver_data(spark, silver_df)

    # Transform and load data from Silver layer into Gold layer
    load_gold_data(spark, silver_df)

if __name__ == "__main__":
    main()