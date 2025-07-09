# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import dlt
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# COMMAND ----------

# Configuration
CUSTOMER_CSV_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_CSV_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
CUSTOMER_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customer"
ORDER_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.order"
ORDER_SUMMARY_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"
CUSTOMER_AGGREGATE_SPEND_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend"
ERROR_LOG_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.error_logs"

# COMMAND ----------

# Helper function for error logging
def log_error(error_message, source_function, additional_info=None):
    """Log errors to the error_logs table"""
    try:
        error_df = spark.createDataFrame([
            {
                "timestamp": datetime.now().isoformat(),
                "error_message": str(error_message),
                "source_function": source_function,
                "additional_info": str(additional_info) if additional_info else None
            }
        ])
        
        error_df.write.format("delta").mode("append").saveAsTable(ERROR_LOG_TABLE)
        logger.error(f"Error in {source_function}: {error_message}")
    except Exception as e:
        logger.error(f"Failed to log error: {e}")

# COMMAND ----------

# TR-DLT-001: Load Customer Data to Delta Table
@dlt.table(
    name="customer",
    comment="Customer data loaded from CSV",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer():
    try:
        logger.info("Loading customer data from CSV")
        return (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(CUSTOMER_CSV_PATH)
            .dropDuplicates()
            .dropna()
        )
    except Exception as e:
        log_error(e, "customer_load", CUSTOMER_CSV_PATH)
        raise e

# COMMAND ----------

# TR-DLT-001: Load Order Data to Delta Table
@dlt.table(
    name="order",
    comment="Order data loaded from CSV",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def order():
    try:
        logger.info("Loading order data from CSV")
        return (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(ORDER_CSV_PATH)
            .dropDuplicates()
            .dropna()
        )
    except Exception as e:
        log_error(e, "order_load", ORDER_CSV_PATH)
        raise e

# COMMAND ----------

# TR-DLT-002: Create ordersummary Table by joining customer and order data
@dlt.table(
    name="ordersummary",
    comment="Joined customer and order data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def ordersummary():
    try:
        logger.info("Creating ordersummary table by joining customer and order data")
        
        # Read from the DLT tables
        customer_df = dlt.read("customer")
        order_df = dlt.read("order")
        
        # Add SCD Type 2 fields
        current_timestamp = F.current_timestamp()
        
        result_df = (
            customer_df.join(
                order_df,
                customer_df.CustId == order_df.CustId,
                "inner"
            )
            .withColumn("StartDate", current_timestamp)
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
            .withColumn("IsActive", F.lit(True))
        )
        
        return result_df
    except Exception as e:
        log_error(e, "ordersummary_create")
        raise e

# COMMAND ----------

# TR-DLT-003: Implement SCD Type 2 Logic for ordersummary Table
@dlt.table(
    name="ordersummary_scd2",
    comment="Ordersummary table with SCD Type 2 logic",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def ordersummary_scd2():
    try:
        logger.info("Implementing SCD Type 2 logic for ordersummary table")
        
        # Read the current state of the tables
        customer_df = dlt.read("customer")
        ordersummary_df = dlt.read("ordersummary")
        
        # Check if this is the first run (if ordersummary is empty)
        if ordersummary_df.isEmpty():
            return ordersummary_df
        
        # Get the existing customer data from ordersummary
        existing_customers = ordersummary_df.select("CustId", "Name", "Address").distinct()
        
        # Find changed customers
        changed_customers = (
            customer_df.select("CustId", "Name", "Address")
            .exceptAll(existing_customers)
            .join(customer_df, ["CustId"], "inner")
        )
        
        # If no changes, return the original table
        if changed_customers.isEmpty():
            return ordersummary_df
            
        # Get the current active records that need to be expired
        customers_to_expire = (
            ordersummary_df
            .filter(F.col("IsActive") == True)
            .join(changed_customers.select("CustId"), ["CustId"], "inner")
        )
        
        # Expire the old records
        expired_records = (
            customers_to_expire
            .withColumn("EndDate", F.current_timestamp())
            .withColumn("IsActive", F.lit(False))
        )
        
        # Create new active records
        order_data = dlt.read("order")
        new_records = (
            changed_customers.join(
                order_data,
                changed_customers.CustId == order_data.CustId,
                "inner"
            )
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
            .withColumn("IsActive", F.lit(True))
        )
        
        # Combine unchanged records, expired records, and new records
        unchanged_records = ordersummary_df.filter(
            ~F.col("CustId").isin([r.CustId for r in changed_customers.select("CustId").collect()])
        )
        
        result_df = unchanged_records.union(expired_records).union(new_records)
        
        return result_df
    except Exception as e:
        log_error(e, "ordersummary_scd2")
        raise e

# COMMAND ----------

# TR-DLT-004: Create customeraggregatespend Table with aggregated data
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spend data",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customeraggregatespend():
    try:
        logger.info("Creating customeraggregatespend table with aggregated data")
        
        # Read from the SCD2 ordersummary table
        ordersummary_df = dlt.read("ordersummary_scd2")
        
        # Only use active records for aggregation
        active_records = ordersummary_df.filter(F.col("IsActive") == True)
        
        # Aggregate TotalAmount by Name and Date
        result_df = (
            active_records
            .groupBy("Name", "Date")
            .agg(F.sum("TotalAmount").alias("TotalSpend"))
            .withColumn("LastUpdated", F.current_timestamp())
        )
        
        return result_df
    except Exception as e:
        log_error(e, "customeraggregatespend_create")
        raise e