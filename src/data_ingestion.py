"""
Data ingestion module for loading customer and order data from CSV files into Delta tables
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
import logging

from src.config import (
    CUSTOMER_DATA_PATH,
    ORDER_DATA_PATH,
    CUSTOMER_TABLE,
    ORDER_TABLE,
    CUSTOMER_SCHEMA,
    ORDER_SCHEMA
)
from src.utils import get_spark_session, log_info, log_error

def create_customer_schema():
    """
    Create the schema for customer data
    
    Returns:
        StructType: The schema for customer data
    """
    return StructType([
        StructField("CustId", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

def create_order_schema():
    """
    Create the schema for order data
    
    Returns:
        StructType: The schema for order data
    """
    return StructType([
        StructField("OrderId", IntegerType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", IntegerType(), True)
    ])

def read_customer_data() -> DataFrame:
    """
    Read customer data from CSV files
    
    Returns:
        DataFrame: Customer data
    """
    spark = get_spark_session()
    try:
        log_info(f"Reading customer data from {CUSTOMER_DATA_PATH}")
        return spark.read.format("csv") \
            .option("header", "true") \
            .schema(create_customer_schema()) \
            .load(CUSTOMER_DATA_PATH)
    except Exception as e:
        log_error("Error reading customer data", e)
        raise

def read_order_data() -> DataFrame:
    """
    Read order data from CSV files
    
    Returns:
        DataFrame: Order data
    """
    spark = get_spark_session()
    try:
        log_info(f"Reading order data from {ORDER_DATA_PATH}")
        return spark.read.format("csv") \
            .option("header", "true") \
            .schema(create_order_schema()) \
            .load(ORDER_DATA_PATH)
    except Exception as e:
        log_error("Error reading order data", e)
        raise

def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Write data to a Delta table
    
    Args:
        df (DataFrame): The data to write
        table_name (str): The name of the Delta table
    """
    try:
        log_info(f"Writing data to {table_name}")
        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)
        log_info(f"Successfully wrote data to {table_name}")
    except Exception as e:
        log_error(f"Error writing data to {table_name}", e)
        raise

def ingest_data():
    """
    Ingest customer and order data from CSV files into Delta tables
    """
    try:
        log_info("Starting data ingestion process")
        
        # Read customer data
        customer_df = read_customer_data()
        log_info(f"Read {customer_df.count()} customer records")
        
        # Read order data
        order_df = read_order_data()
        log_info(f"Read {order_df.count()} order records")
        
        # Write to Delta tables
        write_to_delta_table(customer_df, CUSTOMER_TABLE)
        write_to_delta_table(order_df, ORDER_TABLE)
        
        log_info("Data ingestion process completed successfully")
    except Exception as e:
        log_error("Data ingestion process failed", e)
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    ingest_data()