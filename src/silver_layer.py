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
    """Create necessary catalogs and schemas for the silver layer"""
    try:
        # Create silver catalog if it doesn't exist
        spark.sql("CREATE CATALOG IF NOT EXISTS silverzone")
        
        # Create schema in silver catalog
        spark.sql("CREATE SCHEMA IF NOT EXISTS silverzone.data")
    except Exception as e:
        log_error("Failed to create catalogs and schemas", str(e), "silver_setup")

@dlt.table(
    name="customer_order_combined",
    comment="Transformed and joined customer and order data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({"valid_id": "id IS NOT NULL"})
def customer_order_combined():
    """
    Join customer and order data, clean nulls and duplicates,
    and implement SCD Type-2 tracking
    """
    try:
        # Read from bronze tables
        customer_df = dlt.read("customer_raw")
        orders_df = dlt.read("orders_raw")
        
        # Join datasets on id column
        joined_df = customer_df.join(orders_df, "id", "inner")
        
        # Remove records with null values
        joined_df = joined_df.dropna()
        
        # Remove duplicate records
        joined_df = joined_df.dropDuplicates()
        
        # Add SCD Type-2 tracking columns
        joined_df = joined_df.withColumn("CreateDateTime", current_timestamp()) \
                             .withColumn("UpdateDateTime", current_timestamp()) \
                             .withColumn("IsActive", lit(True))
        
        return joined_df
    except Exception as e:
        log_error("Failed to create customer_order_combined", str(e), "customer_order_combined")
        # Return empty dataframe with expected schema
        return spark.createDataFrame([], """
            id STRING, name STRING, email STRING, age INT, 
            order_id STRING, amount DOUBLE, order_date TIMESTAMP,
            CreateDateTime TIMESTAMP, UpdateDateTime TIMESTAMP, IsActive BOOLEAN
        """)

# Initialize catalogs and schemas
create_catalogs_and_schemas()