"""
Module to cleanse data in Delta tables (TR-DELTA-002)
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.config import (
    CUSTOMER_DELTA_TABLE,
    ORDER_DELTA_TABLE
)
from src.utils import log_info, log_error

def cleanse_delta_tables():
    """
    Remove null values and duplicate records from customer and order Delta tables
    """
    spark = SparkSession.builder.appName("CleanseDeltaTables").getOrCreate()
    
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Cleanse customer data
        log_info(f"Cleansing customer data in {CUSTOMER_DELTA_TABLE}")
        try:
            # Read customer data
            customer_df = spark.read.format("delta").table(CUSTOMER_DELTA_TABLE)
            
            # Get column names for customer table
            customer_columns = customer_df.columns
            
            # Remove rows with null values in any column
            log_info("Removing null values from customer data")
            for column in customer_columns:
                customer_df = customer_df.filter(col(column).isNotNull())
            
            # Remove duplicate records based on CustId
            log_info("Removing duplicate records from customer data")
            customer_df = customer_df.dropDuplicates(["CustId"])
            
            # Write cleansed data back to Delta table
            log_info(f"Writing cleansed customer data back to {CUSTOMER_DELTA_TABLE}")
            customer_df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(CUSTOMER_DELTA_TABLE)
            
            log_info(f"Successfully cleansed customer data in {CUSTOMER_DELTA_TABLE}")
        except Exception as e:
            log_error("Error cleansing customer data", e)
            raise
        
        # Cleanse order data
        log_info(f"Cleansing order data in {ORDER_DELTA_TABLE}")
        try:
            # Read order data
            order_df = spark.read.format("delta").table(ORDER_DELTA_TABLE)
            
            # Get column names for order table
            order_columns = order_df.columns
            
            # Remove rows with null values in any column
            log_info("Removing null values from order data")
            for column in order_columns:
                order_df = order_df.filter(col(column).isNotNull())
            
            # Remove duplicate records based on OrderId
            log_info("Removing duplicate records from order data")
            order_df = order_df.dropDuplicates(["OrderId"])
            
            # Write cleansed data back to Delta table
            log_info(f"Writing cleansed order data back to {ORDER_DELTA_TABLE}")
            order_df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(ORDER_DELTA_TABLE)
            
            log_info(f"Successfully cleansed order data in {ORDER_DELTA_TABLE}")
        except Exception as e:
            log_error("Error cleansing order data", e)
            raise
            
    except Exception as e:
        log_error("Error in cleanse_delta_tables", e)
        raise

if __name__ == "__main__":
    cleanse_delta_tables()