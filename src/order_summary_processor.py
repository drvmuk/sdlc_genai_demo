"""
Module for creating and maintaining the order summary table.
Implements TR-ORD-003, TR-ORD-004, and TR-ORD-005.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType
import time

from src.config import (
    CUSTOMER_DELTA_TABLE, ORDER_DELTA_TABLE, ORDER_SUMMARY_TABLE,
    EFFECTIVE_FROM_COL, EFFECTIVE_TO_COL, IS_CURRENT_COL, INFINITE_DATE
)
from src.logger import get_logger, log_job_start, log_job_end

logger = get_logger(__name__)

def create_spark_session():
    """Create and return a SparkSession"""
    return SparkSession.builder \
        .appName("Order Summary Processor") \
        .getOrCreate()

def create_order_summary_table_if_not_exists(spark):
    """
    Create the order summary table if it doesn't exist
    
    Args:
        spark: SparkSession
    """
    logger.info(f"Checking if table {ORDER_SUMMARY_TABLE} exists")
    
    # Check if the table exists
    tables = spark.sql(f"SHOW TABLES IN {'.'.join(ORDER_SUMMARY_TABLE.split('.')[:2])}")
    table_exists = tables.filter(col("tableName") == ORDER_SUMMARY_TABLE.split('.')[-1]).count() > 0
    
    if not table_exists:
        logger.info(f"Creating table {ORDER_SUMMARY_TABLE}")
        
        # Define schema for the order summary table
        schema = StructType([
            StructField("OrderId", StringType(), False),
            StructField("CustId", StringType(), False),
            StructField("CustomerName", StringType(), True),
            StructField("CustomerAddress", StringType(), True),
            StructField("CustomerEmail", StringType(), True),
            StructField("OrderDate", DateType(), True),
            StructField("TotalAmount", DoubleType(), True),
            StructField("OrderStatus", StringType(), True),
            StructField(EFFECTIVE_FROM_COL, DateType(), False),
            StructField(EFFECTIVE_TO_COL, DateType(), False),
            StructField(IS_CURRENT_COL, BooleanType(), False),
            StructField("last_updated", TimestampType(), False)
        ])
        
        # Create empty DataFrame with the defined schema
        empty_df = spark.createDataFrame([], schema)
        
        # Write the empty DataFrame as a Delta table
        empty_df.write.format("delta").saveAsTable(ORDER_SUMMARY_TABLE)
        logger.info(f"Successfully created table {ORDER_SUMMARY_TABLE}")
    else:
        logger.info(f"Table {ORDER_SUMMARY_TABLE} already exists")

def load_order_summary_data(spark):
    """
    Join customer and order data and load it into the order summary table as SCD Type 2
    
    Args:
        spark: SparkSession
    """
    logger.info("Loading order summary data")
    
    try:
        # Read customer and order data
        customer_df = spark.read.format("delta").table(CUSTOMER_DELTA_TABLE)
        order_df = spark.read.format("delta").table(ORDER_DELTA_TABLE)
        
        # Join customer and order data
        joined_df = order_df.join(customer_df, "CustId", "inner")
        
        # Read current order summary data
        current_summary_df = spark.read.format("delta").table(ORDER_SUMMARY_TABLE)
        
        # Prepare new data with SCD Type 2 columns
        new_data = joined_df.select(
            order_df["OrderId"],
            order_df["CustId"],
            customer_df["CustomerName"],
            customer_df["CustomerAddress"],
            customer_df["CustomerEmail"],
            order_df["OrderDate"],
            order_df["TotalAmount"],
            order_df["OrderStatus"],
            current_date().alias(EFFECTIVE_FROM_COL),
            lit(INFINITE_DATE).cast("date").alias(EFFECTIVE_TO_COL),
            lit(True).alias(IS_CURRENT_COL),
            current_timestamp().alias("last_updated")
        )
        
        # If there's existing data, implement SCD Type 2 logic
        if current_summary_df.count() > 0:
            # Identify existing records that need to be updated (marked as not current)
            existing_keys = current_summary_df.filter(col(IS_CURRENT_COL) == True) \
                .select("OrderId", "CustId") \
                .join(new_data.select("OrderId", "CustId"), ["OrderId", "CustId"], "inner")
            
            # Update existing records to mark them as not current
            if existing_keys.count() > 0:
                logger.info(f"Updating {existing_keys.count()} existing records")
                
                # Get records to update
                to_update = current_summary_df.join(
                    existing_keys,
                    ["OrderId", "CustId"],
                    "inner"
                ).filter(col(IS_CURRENT_COL) == True)
                
                # Update the effective_to date and is_current flag
                updated_records = to_update.withColumn(EFFECTIVE_TO_COL, current_date()) \
                    .withColumn(IS_CURRENT_COL, lit(False)) \
                    .withColumn("last_updated", current_timestamp())
                
                # Get records that don't need to be updated
                unchanged_records = current_summary_df.join(
                    existing_keys,
                    ["OrderId", "CustId"],
                    "left_anti"
                )
                
                # Combine unchanged records, updated records, and new data
                final_df = unchanged_records.union(updated_records).union(new_data)
                
                # Write back to the order summary table
                final_df.write.format("delta").mode("overwrite").saveAsTable(ORDER_SUMMARY_TABLE)
            else:
                # Just append new data if no existing records need to be updated
                new_data.write.format("delta").mode("append").saveAsTable(ORDER_SUMMARY_TABLE)
        else:
            # If no existing data, just write the new data
            new_data.write.format("delta").mode("overwrite").saveAsTable(ORDER_SUMMARY_TABLE)
        
        logger.info("Successfully loaded order summary data")
        
    except Exception as e:
        logger.error(f"Error loading order summary data: {str(e)}")
        raise

def update_order_summary_on_customer_changes(spark):
    """
    Update the order summary table when there are changes to the customer table
    
    Args:
        spark: SparkSession
    """
    logger.info("Updating order summary based on customer changes")
    
    try:
        # Read customer data
        customer_df = spark.read.format("delta").table(CUSTOMER_DELTA_TABLE)
        
        # Read current order summary data
        order_summary_df = spark.read.format("delta").table(ORDER_SUMMARY_TABLE)
        
        # Only process if there's existing data
        if order_summary_df.count() > 0:
            # Join to find records that need updating
            # Compare customer fields to detect changes
            joined_df = order_summary_df.filter(col(IS_CURRENT_COL) == True) \
                .join(
                    customer_df,
                    "CustId",
                    "inner"
                )
            
            # Find records where customer data has changed
            changed_records = joined_df.filter(
                (col("CustomerName") != col("customer.CustomerName")) |
                (col("CustomerAddress") != col("customer.CustomerAddress")) |
                (col("CustomerEmail") != col("customer.CustomerEmail"))
            ).select(order_summary_df["CustId"]).distinct()
            
            # If there are changes, update the order summary table
            if changed_records.count() > 0:
                logger.info(f"Found {changed_records.count()} customers with changes")
                
                # Get records to update
                to_update = order_summary_df.join(
                    changed_records,
                    "CustId",
                    "inner"
                ).filter(col(IS_CURRENT_COL) == True)
                
                # Update the effective_to date and is_current flag
                updated_records = to_update.withColumn(EFFECTIVE_TO_COL, current_date()) \
                    .withColumn(IS_CURRENT_COL, lit(False)) \
                    .withColumn("last_updated", current_timestamp())
                
                # Create new records with updated customer information
                new_records = to_update.join(
                    customer_df,
                    "CustId",
                    "inner"
                ).select(
                    to_update["OrderId"],
                    to_update["CustId"],
                    customer_df["CustomerName"],
                    customer_df["CustomerAddress"],
                    customer_df["CustomerEmail"],
                    to_update["OrderDate"],
                    to_update["TotalAmount"],
                    to_update["OrderStatus"],
                    current_date().alias(EFFECTIVE_FROM_COL),
                    lit(INFINITE_DATE).cast("date").alias(EFFECTIVE_TO_COL),
                    lit(True).alias(IS_CURRENT_COL),
                    current_timestamp().alias("last_updated")
                )
                
                # Get records that don't need to be updated
                unchanged_records = order_summary_df.join(
                    changed_records,
                    "CustId",
                    "left_anti"
                )
                
                # Combine unchanged records, updated records, and new records
                final_df = unchanged_records.union(updated_records).union(new_records)
                
                # Write back to the order summary table
                final_df.write.format("delta").mode("overwrite").saveAsTable(ORDER_SUMMARY_TABLE)
                logger.info("Successfully updated order summary with customer changes")
            else:
                logger.info("No customer changes detected")
        else:
            logger.info("Order summary table is empty, no updates needed")
        
    except Exception as e:
        logger.error(f"Error updating order summary with customer changes: {str(e)}")
        raise

def main():
    """Main function to create and maintain the order summary table"""
    job_name = "Order Summary Processing"
    log_job_start(logger, job_name)
    
    try:
        spark = create_spark_session()
        
        # Create order summary table if it doesn't exist (TR-ORD-003)
        create_order_summary_table_if_not_exists(spark)
        
        # Load order summary data (TR-ORD-004)
        load_order_summary_data(spark)
        
        # Update order summary on customer changes (TR-ORD-005)
        update_order_summary_on_customer_changes(spark)
        
        log_job_end(logger, job_name)
        
    except Exception as e:
        logger.error(f"Error in {job_name}: {str(e)}")
        raise

if __name__ == "__main__":
    main()