# Databricks notebook source
import dlt
from pyspark.sql.functions import current_timestamp, col, avg, sum, round, regexp_extract

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
    """Create necessary catalogs and schemas for the gold layer"""
    try:
        # Create gold catalog if it doesn't exist
        spark.sql("CREATE CATALOG IF NOT EXISTS goldzone")
        
        # Create schema in gold catalog
        spark.sql("CREATE SCHEMA IF NOT EXISTS goldzone.data")
    except Exception as e:
        log_error("Failed to create catalogs and schemas", str(e), "gold_setup")

@dlt.table(
    name="customer_order_summary_by_age",
    comment="Aggregated customer and order data by age",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_order_summary_by_age():
    """
    Aggregate customer and order data by age group
    Calculate total revenue and average order amount
    """
    try:
        # Read from silver layer
        df = dlt.read("customer_order_combined")
        
        # Group by age and calculate metrics
        summary_df = df.groupBy("age") \
            .agg(
                round(sum("amount"), 2).alias("total_revenue"),
                round(avg("amount"), 2).alias("avg_order_amount"),
                count("order_id").alias("order_count")
            ) \
            .withColumn("last_updated", current_timestamp())
        
        return summary_df
    except Exception as e:
        log_error("Failed to create customer_order_summary_by_age", str(e), "customer_order_summary_by_age")
        # Return empty dataframe with expected schema
        return spark.createDataFrame([], "age INT, total_revenue DOUBLE, avg_order_amount DOUBLE, order_count LONG, last_updated TIMESTAMP")

@dlt.table(
    name="customer_order_summary_by_domain",
    comment="Aggregated customer and order data by email domain",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_order_summary_by_domain():
    """
    Aggregate customer and order data by email domain
    Calculate total revenue and average order amount
    """
    try:
        # Read from silver layer
        df = dlt.read("customer_order_combined")
        
        # Extract email domain and group by domain
        domain_df = df.withColumn("email_domain", regexp_extract(col("email"), "@([^.]+)", 1))
        
        summary_df = domain_df.groupBy("email_domain") \
            .agg(
                round(sum("amount"), 2).alias("total_revenue"),
                round(avg("amount"), 2).alias("avg_order_amount"),
                count("order_id").alias("order_count")
            ) \
            .withColumn("last_updated", current_timestamp())
        
        return summary_df
    except Exception as e:
        log_error("Failed to create customer_order_summary_by_domain", str(e), "customer_order_summary_by_domain")
        # Return empty dataframe with expected schema
        return spark.createDataFrame([], "email_domain STRING, total_revenue DOUBLE, avg_order_amount DOUBLE, order_count LONG, last_updated TIMESTAMP")

# Initialize catalogs and schemas
create_catalogs_and_schemas()