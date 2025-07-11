"""
Module to cleanse customer and order data by removing null and duplicate records.
"""
from pyspark.sql import SparkSession, DataFrame
import logging
from src.config import CUSTOMER_DELTA_TABLE, ORDER_DELTA_TABLE
from src.utils import setup_logger, read_delta_table, write_to_delta_table

logger = setup_logger("cleanse_data")

def remove_null_records(df: DataFrame, key_columns: list = None) -> DataFrame:
    """
    Remove null records from a DataFrame.
    
    Args:
        df: The input DataFrame
        key_columns: List of key columns to check for nulls (if None, checks all columns)
        
    Returns:
        DataFrame: The DataFrame with null records removed
    """
    try:
        original_count = df.count()
        
        if key_columns:
            cleansed_df = df.dropna(subset=key_columns)
        else:
            cleansed_df = df.na.drop()
        
        cleansed_count = cleansed_df.count()
        removed_count = original_count - cleansed_count
        
        logger.info(f"Removed {removed_count} null records out of {original_count}")
        return cleansed_df
    except Exception as e:
        logger.error(f"Error removing null records: {str(e)}")
        raise

def remove_duplicate_records(df: DataFrame, key_columns: list) -> DataFrame:
    """
    Remove duplicate records from a DataFrame.
    
    Args:
        df: The input DataFrame
        key_columns: List of columns to use for identifying duplicates
        
    Returns:
        DataFrame: The DataFrame with duplicate records removed
    """
    try:
        original_count = df.count()
        
        cleansed_df = df.dropDuplicates(key_columns)
        
        cleansed_count = cleansed_df.count()
        removed_count = original_count - cleansed_count
        
        logger.info(f"Removed {removed_count} duplicate records out of {original_count}")
        return cleansed_df
    except Exception as e:
        logger.error(f"Error removing duplicate records: {str(e)}")
        raise

def cleanse_customer_data(customer_df: DataFrame) -> DataFrame:
    """
    Cleanse customer data by removing null and duplicate records.
    
    Args:
        customer_df: The customer DataFrame
        
    Returns:
        DataFrame: The cleansed customer DataFrame
    """
    try:
        # Remove null records from key columns
        customer_key_columns = ["CustId", "Name"]
        customer_df = remove_null_records(customer_df, customer_key_columns)
        
        # Remove duplicate records based on CustId
        customer_df = remove_duplicate_records(customer_df, ["CustId"])
        
        logger.info("Successfully cleansed customer data")
        return customer_df
    except Exception as e:
        logger.error(f"Error cleansing customer data: {str(e)}")
        raise

def cleanse_order_data(order_df: DataFrame) -> DataFrame:
    """
    Cleanse order data by removing null and duplicate records.
    
    Args:
        order_df: The order DataFrame
        
    Returns:
        DataFrame: The cleansed order DataFrame
    """
    try:
        # Remove null records from key columns
        order_key_columns = ["OrderId", "CustId", "PricePerUnit", "Qty"]
        order_df = remove_null_records(order_df, order_key_columns)
        
        # Remove duplicate records based on OrderId
        order_df = remove_duplicate_records(order_df, ["OrderId"])
        
        logger.info("Successfully cleansed order data")
        return order_df
    except Exception as e:
        logger.error(f"Error cleansing order data: {str(e)}")
        raise

def main():
    """
    Main function to cleanse customer and order data.
    """
    try:
        spark = SparkSession.builder.appName("Cleanse Data").getOrCreate()
        
        # Read customer data from Delta table
        logger.info(f"Reading customer data from {CUSTOMER_DELTA_TABLE}")
        customer_df = read_delta_table(CUSTOMER_DELTA_TABLE)
        
        # Cleanse customer data
        cleansed_customer_df = cleanse_customer_data(customer_df)
        
        # Write cleansed customer data back to Delta table
        write_to_delta_table(cleansed_customer_df, CUSTOMER_DELTA_TABLE)
        logger.info(f"Successfully wrote cleansed customer data to {CUSTOMER_DELTA_TABLE}")
        
        # Read order data from Delta table
        logger.info(f"Reading order data from {ORDER_DELTA_TABLE}")
        order_df = read_delta_table(ORDER_DELTA_TABLE)
        
        # Cleanse order data
        cleansed_order_df = cleanse_order_data(order_df)
        
        # Write cleansed order data back to Delta table
        write_to_delta_table(cleansed_order_df, ORDER_DELTA_TABLE)
        logger.info(f"Successfully wrote cleansed order data to {ORDER_DELTA_TABLE}")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise
    finally:
        logger.info("Data cleansing process completed")

if __name__ == "__main__":
    main()