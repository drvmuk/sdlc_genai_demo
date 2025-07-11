"""
Module to load customer and order data from CSV files to Delta tables.
"""
from pyspark.sql import SparkSession, DataFrame
import logging
from src.config import CUSTOMER_CSV_PATH, ORDER_CSV_PATH, CUSTOMER_DELTA_TABLE, ORDER_DELTA_TABLE
from src.utils import setup_logger, write_to_delta_table, handle_null_data

logger = setup_logger("load_data")

def load_customer_data(spark: SparkSession) -> DataFrame:
    """
    Load customer data from CSV to a DataFrame.
    
    Args:
        spark: The SparkSession
        
    Returns:
        DataFrame: The customer data
    """
    try:
        logger.info(f"Reading customer data from {CUSTOMER_CSV_PATH}")
        customer_df = spark.read.option("header", "true") \
                                .option("inferSchema", "true") \
                                .csv(CUSTOMER_CSV_PATH)
        
        logger.info(f"Customer data schema: {customer_df.schema}")
        return customer_df
    except Exception as e:
        logger.error(f"Error loading customer data: {str(e)}")
        raise

def load_order_data(spark: SparkSession) -> DataFrame:
    """
    Load order data from CSV to a DataFrame.
    
    Args:
        spark: The SparkSession
        
    Returns:
        DataFrame: The order data
    """
    try:
        logger.info(f"Reading order data from {ORDER_CSV_PATH}")
        order_df = spark.read.option("header", "true") \
                             .option("inferSchema", "true") \
                             .csv(ORDER_CSV_PATH)
        
        logger.info(f"Order data schema: {order_df.schema}")
        return order_df
    except Exception as e:
        logger.error(f"Error loading order data: {str(e)}")
        raise

def main():
    """
    Main function to load customer and order data to Delta tables.
    """
    try:
        spark = SparkSession.builder.appName("Load Data to Delta Tables").getOrCreate()
        
        # Load customer data
        customer_df = load_customer_data(spark)
        
        # Basic validation
        if customer_df.count() == 0:
            logger.warning("Customer data is empty")
        else:
            # Write customer data to Delta table
            write_to_delta_table(customer_df, CUSTOMER_DELTA_TABLE)
            logger.info(f"Successfully loaded customer data to {CUSTOMER_DELTA_TABLE}")
        
        # Load order data
        order_df = load_order_data(spark)
        
        # Basic validation
        if order_df.count() == 0:
            logger.warning("Order data is empty")
        else:
            # Write order data to Delta table
            write_to_delta_table(order_df, ORDER_DELTA_TABLE)
            logger.info(f"Successfully loaded order data to {ORDER_DELTA_TABLE}")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise
    finally:
        logger.info("Data loading process completed")

if __name__ == "__main__":
    main()