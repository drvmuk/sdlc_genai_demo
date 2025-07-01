# Databricks notebook source
import dlt
from pyspark.sql.functions import current_timestamp, lit, col

# Error logging function
def log_error(error_message, error_details, table_name):
    """Log errors to a designated error log table"""
    spark.sql("""
    CREATE TABLE IF NOT EXISTS goldzone.data.error_logs (
        error_timestamp TIMESTAMP,
        error_message STRING,
        error_details STRING,
        source_table STRING
    ) USING DELTA
    """)
    
    error_df = spark.createDataFrame(
        [(current_timestamp(), error_message, error_details, table_name)],
        ["error_timestamp", "error_message", "error_details", "source_table"]
    )
    
    error_df.write.format("delta").mode("append").saveAsTable("goldzone.data.error_logs")

# Create catalogs and schemas if they don't exist
def create_catalogs_and_schemas():
    """Create necessary catalogs and schemas for the bronze layer"""
    try:
        # Create bronze catalog if it doesn't exist
        spark.sql("CREATE CATALOG IF NOT EXISTS bronzezone")
        
        # Create schema in bronze catalog
        spark.sql("CREATE SCHEMA IF NOT EXISTS bronzezone.data")
    except Exception as e:
        log_error("Failed to create catalogs and schemas", str(e), "bronze_setup")

# Define bronze layer tables using DLT
@dlt.table(
    name="customer_raw",
    comment="Raw customer data from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_raw():
    """
    Ingest raw customer data from CSV files using Auto Loader
    Implements SCD Type-2 with tracking columns
    """
    try:
        # Read customer data using Auto Loader
        df = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.inferSchema", "true") \
            .option("header", "true") \
            .load("/Volumes/bronzezone/data/customer_data/")
        
        # Add SCD Type-2 tracking columns
        df = df.withColumn("CreateDateTime", current_timestamp()) \
               .withColumn("UpdateDateTime", current_timestamp()) \
               .withColumn("IsActive", lit(True))
        
        return df
    except Exception as e:
        log_error("Failed to load customer_raw data", str(e), "customer_raw")
        # Return empty dataframe with expected schema
        return spark.createDataFrame([], "id STRING, name STRING, email STRING, age INT, CreateDateTime TIMESTAMP, UpdateDateTime TIMESTAMP, IsActive BOOLEAN")

@dlt.table(
    name="orders_raw",
    comment="Raw order data from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def orders_raw():
    """
    Ingest raw order data from CSV files using Auto Loader
    Implements SCD Type-2 with tracking columns
    """
    try:
        # Read order data using Auto Loader
        df = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.inferSchema", "true") \
            .option("header", "true") \
            .load("/Volumes/bronzezone/data/order_data/")
        
        # Add SCD Type-2 tracking columns
        df = df.withColumn("CreateDateTime", current_timestamp()) \
               .withColumn("UpdateDateTime", current_timestamp()) \
               .withColumn("IsActive", lit(True))
        
        return df
    except Exception as e:
        log_error("Failed to load orders_raw data", str(e), "orders_raw")
        # Return empty dataframe with expected schema
        return spark.createDataFrame([], "id STRING, order_id STRING, amount DOUBLE, order_date TIMESTAMP, CreateDateTime TIMESTAMP, UpdateDateTime TIMESTAMP, IsActive BOOLEAN")

# Initialize catalogs and schemas
create_catalogs_and_schemas()