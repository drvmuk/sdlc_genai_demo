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

@dlt.table(
    name="customer_streaming",
    comment="Real-time streaming ingestion of customer data",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_streaming():
    """
    Real-time streaming ingestion of customer data using Auto Loader
    with incremental load configuration
    """
    try:
        # Read customer data using Auto Loader with streaming options
        df = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.inferSchema", "true") \
            .option("cloudFiles.schemaLocation", "/Volumes/bronzezone/data/customer_schema") \
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
            .option("header", "true") \
            .option("maxFilesPerTrigger", 1000) \
            .option("checkpointLocation", "/Volumes/bronzezone/data/customer_checkpoint") \
            .load("/Volumes/bronzezone/data/customer_data/")
        
        # Add tracking columns
        df = df.withColumn("CreateDateTime", current_timestamp()) \
               .withColumn("UpdateDateTime", current_timestamp()) \
               .withColumn("IsActive", lit(True)) \
               .withColumn("SourceFile", input_file_name())
        
        return df
    except Exception as e:
        log_error("Failed to load customer_streaming data", str(e), "customer_streaming")
        # Return empty dataframe with expected schema
        return spark.createDataFrame([], """
            id STRING, name STRING, email STRING, age INT, 
            CreateDateTime TIMESTAMP, UpdateDateTime TIMESTAMP, 
            IsActive BOOLEAN, SourceFile STRING
        """)

@dlt.table(
    name="orders_streaming",
    comment="Real-time streaming ingestion of order data",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def orders_streaming():
    """
    Real-time streaming ingestion of order data using Auto Loader
    with incremental load configuration
    """
    try:
        # Read order data using Auto Loader with streaming options
        df = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.inferSchema", "true") \
            .option("cloudFiles.schemaLocation", "/Volumes/bronzezone/data/orders_schema") \
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
            .option("header", "true") \
            .option("maxFilesPerTrigger", 1000) \
            .option("checkpointLocation", "/Volumes/bronzezone/data/orders_checkpoint") \
            .load("/Volumes/bronzezone/data/order_data/")
        
        # Add tracking columns
        df = df.withColumn("CreateDateTime", current_timestamp()) \
               .withColumn("UpdateDateTime", current_timestamp()) \
               .withColumn("IsActive", lit(True)) \
               .withColumn("SourceFile", input_file_name())
        
        return df
    except Exception as e:
        log_error("Failed to load orders_streaming data", str(e), "orders_streaming")
        # Return empty dataframe with expected schema
        return spark.createDataFrame([], """
            id STRING, order_id STRING, amount DOUBLE, order_date TIMESTAMP,
            CreateDateTime TIMESTAMP, UpdateDateTime TIMESTAMP, 
            IsActive BOOLEAN, SourceFile STRING
        """)