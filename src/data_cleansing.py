"""
Data cleansing module for removing null and duplicate records from customer and order tables
"""
from pyspark.sql import DataFrame
import logging

from src.config import CUSTOMER_TABLE, ORDER_TABLE
from src.utils import get_spark_session, log_info, log_error

def read_delta_table(table_name: str) -> DataFrame:
    """
    Read data from a Delta table
    
    Args:
        table_name (str): The name of the Delta table
        
    Returns:
        DataFrame: The data from the Delta table
    """
    spark = get_spark_session()
    try:
        log_info(f"Reading data from {table_name}")
        return spark.read.format("delta").table(table_name)
    except Exception as e:
        log_error(f"Error reading data from {table_name}", e)
        raise

def remove_null_records(df: DataFrame, columns: list) -> DataFrame:
    """
    Remove records with null values in specified columns
    
    Args:
        df (DataFrame): The input DataFrame
        columns (list): The columns to check for null values
        
    Returns:
        DataFrame: The DataFrame with null records removed
    """
    log_info(f"Removing null records from columns: {columns}")
    initial_count = df.count()
    
    for column in columns:
        df = df.filter(df[column].isNotNull())
    
    final_count = df.count()
    log_info(f"Removed {initial_count - final_count} null records")
    
    return df

def remove_duplicate_records(df: DataFrame, key_columns: list) -> DataFrame:
    """
    Remove duplicate records based on key columns
    
    Args:
        df (DataFrame): The input DataFrame
        key_columns (list): The columns to use as keys for identifying duplicates
        
    Returns:
        DataFrame: The DataFrame with duplicate records removed
    """
    log_info(f"Removing duplicate records based on columns: {key_columns}")
    initial_count = df.count()
    
    df = df.dropDuplicates(key_columns)
    
    final_count = df.count()
    log_info(f"Removed {initial_count - final_count} duplicate records")
    
    return df

def write_cleansed_data(df: DataFrame, table_name: str):
    """
    Write cleansed data to a Delta table
    
    Args:
        df (DataFrame): The cleansed data
        table_name (str): The name of the Delta table
    """
    try:
        log_info(f"Writing cleansed data to {table_name}")
        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)
        log_info(f"Successfully wrote cleansed data to {table_name}")
    except Exception as e:
        log_error(f"Error writing cleansed data to {table_name}", e)
        raise

def cleanse_customer_data():
    """
    Cleanse customer data by removing null and duplicate records
    """
    try:
        log_info("Starting customer data cleansing")
        
        # Read customer data
        customer_df = read_delta_table(CUSTOMER_TABLE)
        log_info(f"Read {customer_df.count()} customer records")
        
        # Remove null records
        customer_df = remove_null_records(customer_df, ["CustId", "Name", "EmailId"])
        
        # Remove duplicate records
        customer_df = remove_duplicate_records(customer_df, ["CustId"])
        
        # Write cleansed data
        write_cleansed_data(customer_df, CUSTOMER_TABLE)
        
        log_info("Customer data cleansing completed successfully")
    except Exception as e:
        log_error("Customer data cleansing failed", e)
        raise

def cleanse_order_data():
    """
    Cleanse order data by removing null and duplicate records
    """
    try:
        log_info("Starting order data cleansing")
        
        # Read order data
        order_df = read_delta_table(ORDER_TABLE)
        log_info(f"Read {order_df.count()} order records")
        
        # Remove null records
        order_df = remove_null_records(order_df, ["OrderId", "CustId", "ItemName", "PricePerUnit", "Qty"])
        
        # Remove duplicate records
        order_df = remove_duplicate_records(order_df, ["OrderId"])
        
        # Write cleansed data
        write_cleansed_data(order_df, ORDER_TABLE)
        
        log_info("Order data cleansing completed successfully")
    except Exception as e:
        log_error("Order data cleansing failed", e)
        raise

def cleanse_data():
    """
    Cleanse customer and order data
    """
    try:
        log_info("Starting data cleansing process")
        
        cleanse_customer_data()
        cleanse_order_data()
        
        log_info("Data cleansing process completed successfully")
    except Exception as e:
        log_error("Data cleansing process failed", e)
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    cleanse_data()