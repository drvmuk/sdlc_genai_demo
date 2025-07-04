"""
Module for cleansing customer and order data.
"""
from pyspark.sql import DataFrame

from src.config import CLEANSED_CUSTOMER_DATA_PATH, CLEANSED_ORDER_DATA_PATH
from src.logging_utils import log_info, log_error, log_start_job, log_end_job

def remove_null_records(df: DataFrame, columns: list = None) -> DataFrame:
    """
    Remove records with null values in specified columns.
    If columns is None, check all columns.
    
    Args:
        df: DataFrame to cleanse
        columns: List of column names to check for nulls
        
    Returns:
        DataFrame: Cleansed DataFrame
    """
    if columns is None:
        columns = df.columns
    
    initial_count = df.count()
    cleansed_df = df.dropna(subset=columns)
    final_count = cleansed_df.count()
    
    log_info(f"Removed {initial_count - final_count} records with null values")
    return cleansed_df

def remove_duplicate_records(df: DataFrame, key_columns: list) -> DataFrame:
    """
    Remove duplicate records based on key columns.
    
    Args:
        df: DataFrame to cleanse
        key_columns: List of column names that define uniqueness
        
    Returns:
        DataFrame: Cleansed DataFrame
    """
    initial_count = df.count()
    cleansed_df = df.dropDuplicates(key_columns)
    final_count = cleansed_df.count()
    
    log_info(f"Removed {initial_count - final_count} duplicate records")
    return cleansed_df

def cleanse_customer_data(customer_df: DataFrame) -> DataFrame:
    """
    Cleanse customer data.
    
    Args:
        customer_df: Customer DataFrame
        
    Returns:
        DataFrame: Cleansed customer DataFrame
    """
    log_info("Cleansing customer data")
    
    try:
        # Remove records with null values in key fields
        key_fields = ["CustId", "FirstName", "LastName"]
        cleansed_df = remove_null_records(customer_df, key_fields)
        
        # Remove duplicate records based on CustId
        cleansed_df = remove_duplicate_records(cleansed_df, ["CustId"])
        
        # Save cleansed data
        cleansed_df.write.mode("overwrite").parquet(CLEANSED_CUSTOMER_DATA_PATH)
        log_info(f"Saved cleansed customer data to {CLEANSED_CUSTOMER_DATA_PATH}")
        
        return cleansed_df
    
    except Exception as e:
        log_error("Error cleansing customer data", e)
        raise

def cleanse_order_data(order_df: DataFrame) -> DataFrame:
    """
    Cleanse order data.
    
    Args:
        order_df: Order DataFrame
        
    Returns:
        DataFrame: Cleansed order DataFrame
    """
    log_info("Cleansing order data")
    
    try:
        # Remove records with null values in key fields
        key_fields = ["OrderId", "CustId", "OrderDate"]
        cleansed_df = remove_null_records(order_df, key_fields)
        
        # Remove duplicate records based on OrderId
        cleansed_df = remove_duplicate_records(cleansed_df, ["OrderId"])
        
        # Save cleansed data
        cleansed_df.write.mode("overwrite").parquet(CLEANSED_ORDER_DATA_PATH)
        log_info(f"Saved cleansed order data to {CLEANSED_ORDER_DATA_PATH}")
        
        return cleansed_df
    
    except Exception as e:
        log_error("Error cleansing order data", e)
        raise

def cleanse_data(customer_df: DataFrame, order_df: DataFrame) -> tuple:
    """
    Cleanse customer and order data.
    
    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame
        
    Returns:
        tuple: (cleansed_customer_df, cleansed_order_df)
    """
    log_start_job("Data Cleansing")
    
    try:
        # Cleanse customer data
        cleansed_customer_df = cleanse_customer_data(customer_df)
        
        # Cleanse order data
        cleansed_order_df = cleanse_order_data(order_df)
        
        log_end_job("Data Cleansing")
        return cleansed_customer_df, cleansed_order_df
    
    except Exception as e:
        log_error("Data cleansing failed", e)
        raise