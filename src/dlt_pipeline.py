"""
Main Delta Live Tables pipeline implementation.
"""
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, lit, current_timestamp, when

from src.config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH,
    TARGET_CATALOG, TARGET_SCHEMA,
    CUSTOMER_TABLE, ORDER_TABLE, ORDER_SUMMARY_TABLE, CUSTOMER_AGGREGATE_SPEND_TABLE
)
from src.utils import log_execution, remove_nulls_and_duplicates

# Define the target database
target_db = f"{TARGET_CATALOG}.{TARGET_SCHEMA}"

@dlt.table(
    name=CUSTOMER_TABLE,
    comment="Customer data loaded from CSV files"
)
@log_execution
def customer():
    """
    Load customer data from CSV files into a Delta table.
    Implements TR-DLT-001 (customer part).
    """
    # Read customer data from CSV
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(CUSTOMER_DATA_PATH)
    
    # Clean data - remove nulls and duplicates
    df_cleaned = remove_nulls_and_duplicates(df, key_columns=["CustId"])
    
    # Add audit columns
    df_final = df_cleaned \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp())
    
    return df_final

@dlt.table(
    name=ORDER_TABLE,
    comment="Order data loaded from CSV files with calculated TotalAmount"
)
@log_execution
def order():
    """
    Load order data from CSV files into a Delta table.
    Calculate TotalAmount as PricePerUnit * Qty.
    Implements TR-DLT-001 (order part).
    """
    # Read order data from CSV
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(ORDER_DATA_PATH)
    
    # Add TotalAmount column
    df_with_total = df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    # Clean data - remove nulls and duplicates
    df_cleaned = remove_nulls_and_duplicates(df_with_total, key_columns=["OrderId"])
    
    # Add audit columns
    df_final = df_cleaned \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp())
    
    return df_final

@dlt.table(
    name=ORDER_SUMMARY_TABLE,
    comment="Joined customer and order data with SCD Type 2 implementation"
)
@log_execution
def order_summary():
    """
    Create and load ordersummary table with joined data from customer and order tables.
    Implements TR-DLT-002.
    """
    # Reference the customer and order tables
    customer_df = dlt.read(CUSTOMER_TABLE)
    order_df = dlt.read(ORDER_TABLE)
    
    # Join customer and order tables
    joined_df = customer_df.join(order_df, "CustId")
    
    # Add SCD Type 2 columns
    df_with_scd = joined_df \
        .withColumn("StartDate", current_timestamp()) \
        .withColumn("EndDate", lit(None).cast("timestamp")) \
        .withColumn("IsActive", lit(True))
    
    return df_with_scd

@dlt.table(
    name=CUSTOMER_AGGREGATE_SPEND_TABLE,
    comment="Aggregated customer spend data"
)
@log_execution
def customer_aggregate_spend():
    """
    Create and load customeraggregatespend table with aggregated data from ordersummary table.
    Implements TR-DLT-004.
    """
    # Reference the ordersummary table
    order_summary_df = dlt.read(ORDER_SUMMARY_TABLE)
    
    # Filter for active records only
    active_records = order_summary_df.filter(col("IsActive") == True)
    
    # Aggregate TotalAmount by Name and Date
    aggregated_df = active_records \
        .groupBy("Name", "Date") \
        .agg({"TotalAmount": "sum"}) \
        .withColumnRenamed("sum(TotalAmount)", "TotalSpend")
    
    # Add timestamp for when this aggregation was performed
    result_df = aggregated_df.withColumn("AggregatedAt", current_timestamp())
    
    return result_df

@dlt.table_property
def pipelines_properties():
    """
    Define properties for all tables in the pipeline.
    """
    return {
        "quality": "production",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "CustId,OrderId"
    }

# Update ordersummary table on customer changes (TR-DLT-003)
# This is implemented as a DLT update operation
@dlt.apply_changes(
    target=f"{target_db}.{ORDER_SUMMARY_TABLE}",
    source="customer_changes",
    keys=["CustId"],
    sequence_by="updated_at",
    apply_as_deletes=None,
    column_list=["CustId", "Name", "Address", "Phone", "Email", "updated_at"]
)
@log_execution
def update_order_summary():
    """
    Update ordersummary table when customer data changes.
    Implements TR-DLT-003.
    """
    # Get the latest customer data
    latest_customer = dlt.read(CUSTOMER_TABLE)
    
    # Create a view of customer changes
    customer_changes = latest_customer \
        .withColumn("_change_type", lit("update")) \
        .select("CustId", "Name", "Address", "Phone", "Email", "updated_at", "_change_type")
    
    return customer_changes