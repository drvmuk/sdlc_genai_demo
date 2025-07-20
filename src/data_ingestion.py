"""
Data ingestion and processing module.

This module implements TR-DP-001: Data Ingestion and Processing.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH,
    CLEANSED_CUSTOMER_DATA_PATH, CLEANSED_ORDER_DATA_PATH,
    CUSTOMER_MANDATORY_COLUMNS, ORDER_MANDATORY_COLUMNS
)
from utils import setup_logger, validate_dataframe, remove_null_records, remove_duplicate_records

def ingest_customer_data(spark, logger):
    """
    Ingest customer data from source.
    
    Args:
        spark (SparkSession): Spark session
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: Ingested customer data
    """
    logger.info(f"Ingesting customer data from {CUSTOMER_DATA_PATH}")
    try:
        customer_df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(CUSTOMER_DATA_PATH)
        
        logger.info(f"Successfully ingested {customer_df.count()} customer records")
        return customer_df
    except Exception as e:
        logger.error(f"Error ingesting customer data: {str(e)}")
        raise

def ingest_order_data(spark, logger):
    """
    Ingest order data from source.
    
    Args:
        spark (SparkSession): Spark session
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: Ingested order data
    """
    logger.info(f"Ingesting order data from {ORDER_DATA_PATH}")
    try:
        order_df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(ORDER_DATA_PATH)
        
        logger.info(f"Successfully ingested {order_df.count()} order records")
        return order_df
    except Exception as e:
        logger.error(f"Error ingesting order data: {str(e)}")
        raise

def cleanse_customer_data(customer_df, logger):
    """
    Cleanse customer data by removing nulls and duplicates.
    
    Args:
        customer_df (DataFrame): Customer DataFrame
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: Cleansed customer data
    """
    logger.info("Cleansing customer data")
    
    # Validate dataframe
    if not validate_dataframe(customer_df, CUSTOMER_MANDATORY_COLUMNS, logger):
        raise ValueError("Customer data validation failed")
    
    # Remove records with nulls in mandatory columns
    customer_df = remove_null_records(customer_df, CUSTOMER_MANDATORY_COLUMNS, logger)
    
    # Remove duplicate records based on CustId
    customer_df = remove_duplicate_records(customer_df, ["CustId"], logger)
    
    logger.info(f"Customer data cleansing completed. {customer_df.count()} records remaining")
    return customer_df

def cleanse_order_data(order_df, logger):
    """
    Cleanse order data by removing nulls and duplicates.
    
    Args:
        order_df (DataFrame): Order DataFrame
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: Cleansed order data
    """
    logger.info("Cleansing order data")
    
    # Validate dataframe
    if not validate_dataframe(order_df, ORDER_MANDATORY_COLUMNS, logger):
        raise ValueError("Order data validation failed")
    
    # Remove records with nulls in mandatory columns
    order_df = remove_null_records(order_df, ORDER_MANDATORY_COLUMNS, logger)
    
    # Remove duplicate records based on OrderId
    order_df = remove_duplicate_records(order_df, ["OrderId"], logger)
    
    logger.info(f"Order data cleansing completed. {order_df.count()} records remaining")
    return order_df

def save_cleansed_data(customer_df, order_df, logger):
    """
    Save cleansed data to intermediate storage.
    
    Args:
        customer_df (DataFrame): Cleansed customer DataFrame
        order_df (DataFrame): Cleansed order DataFrame
        logger (logging.Logger): Logger instance
    """
    logger.info(f"Saving cleansed customer data to {CLEANSED_CUSTOMER_DATA_PATH}")
    customer_df.write.mode("overwrite").parquet(CLEANSED_CUSTOMER_DATA_PATH)
    
    logger.info(f"Saving cleansed order data to {CLEANSED_ORDER_DATA_PATH}")
    order_df.write.mode("overwrite").parquet(CLEANSED_ORDER_DATA_PATH)
    
    logger.info("Cleansed data saved successfully")

def main():
    """
    Main function to execute the data ingestion and processing job.
    """
    spark = SparkSession.builder \
        .appName("Data Ingestion and Processing") \
        .getOrCreate()
    
    logger = setup_logger("data_ingestion")
    logger.info("Starting data ingestion and processing job")
    
    try:
        # Step 1: Read customer data
        customer_df = ingest_customer_data(spark, logger)
        
        # Step 2: Read order data
        order_df = ingest_order_data(spark, logger)
        
        # Step 3: Apply data cleansing rules
        cleansed_customer_df = cleanse_customer_data(customer_df, logger)
        cleansed_order_df = cleanse_order_data(order_df, logger)
        
        # Step 4: Save cleansed data
        save_cleansed_data(cleansed_customer_df, cleansed_order_df, logger)
        
        logger.info("Data ingestion and processing job completed successfully")
    except Exception as e:
        logger.error(f"Error in data ingestion and processing job: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()