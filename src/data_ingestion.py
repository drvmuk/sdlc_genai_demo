"""
Module for ingesting raw customer and order data from CSV files.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

from src.config import CUSTOMER_DATA_PATH, ORDER_DATA_PATH
from src.logging_utils import log_info, log_error, log_start_job, log_end_job

def get_spark_session() -> SparkSession:
    """
    Get or create a Spark session.
    
    Returns:
        SparkSession: Active Spark session
    """
    return SparkSession.builder.getOrCreate()

def validate_schema(df: DataFrame, expected_columns: list) -> bool:
    """
    Validate that DataFrame contains all expected columns.
    
    Args:
        df: DataFrame to validate
        expected_columns: List of expected column names
        
    Returns:
        bool: True if schema is valid, False otherwise
    """
    actual_columns = df.columns
    missing_columns = [col for col in expected_columns if col not in actual_columns]
    
    if missing_columns:
        log_error(f"Schema validation failed. Missing columns: {missing_columns}")
        return False
    
    return True

def read_customer_data() -> DataFrame:
    """
    Read customer data from CSV file.
    
    Returns:
        DataFrame: Customer data
    """
    log_info(f"Reading customer data from {CUSTOMER_DATA_PATH}")
    spark = get_spark_session()
    
    try:
        # Define expected schema for customer data
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("Phone", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
            StructField("ZipCode", StringType(), True)
        ])
        
        # Read customer data
        customer_df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .schema(customer_schema) \
            .load(CUSTOMER_DATA_PATH)
        
        # Validate schema
        expected_columns = ["CustId", "FirstName", "LastName", "Email", "Phone", 
                           "Address", "City", "State", "ZipCode"]
        if not validate_schema(customer_df, expected_columns):
            raise ValueError("Customer data schema validation failed")
        
        log_info(f"Successfully read {customer_df.count()} customer records")
        return customer_df
    
    except Exception as e:
        log_error("Error reading customer data", e)
        raise

def read_order_data() -> DataFrame:
    """
    Read order data from CSV file.
    
    Returns:
        DataFrame: Order data
    """
    log_info(f"Reading order data from {ORDER_DATA_PATH}")
    spark = get_spark_session()
    
    try:
        # Define expected schema for order data
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("CustId", StringType(), True),
            StructField("OrderDate", DateType(), True),
            StructField("ShipDate", DateType(), True),
            StructField("OrderTotal", DoubleType(), True),
            StructField("OrderStatus", StringType(), True)
        ])
        
        # Read order data
        order_df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .schema(order_schema) \
            .load(ORDER_DATA_PATH)
        
        # Validate schema
        expected_columns = ["OrderId", "CustId", "OrderDate", "ShipDate", 
                           "OrderTotal", "OrderStatus"]
        if not validate_schema(order_df, expected_columns):
            raise ValueError("Order data schema validation failed")
        
        log_info(f"Successfully read {order_df.count()} order records")
        return order_df
    
    except Exception as e:
        log_error("Error reading order data", e)
        raise

def ingest_data() -> tuple:
    """
    Ingest customer and order data.
    
    Returns:
        tuple: (customer_df, order_df)
    """
    log_start_job("Data Ingestion")
    
    try:
        # Read customer data
        customer_df = read_customer_data()
        
        # Read order data
        order_df = read_order_data()
        
        log_end_job("Data Ingestion")
        return customer_df, order_df
    
    except Exception as e:
        log_error("Data ingestion failed", e)
        raise