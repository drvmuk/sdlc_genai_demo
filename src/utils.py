"""
Utility functions for the Databricks Delta Live Tables Pipeline.
"""
import logging
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, List, Dict, Any, Union
import datetime

def get_spark_session() -> SparkSession:
    """
    Get or create a SparkSession.
    
    Returns:
        SparkSession: The current SparkSession
    """
    return SparkSession.builder.getOrCreate()

def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Set up a logger with the specified name and level.
    
    Args:
        name: The name of the logger
        level: The logging level (default: INFO)
        
    Returns:
        logging.Logger: The configured logger
    """
    logger = logging.getLogger(name)
    
    # Set the logging level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    logger.setLevel(numeric_level)
    
    # Create a console handler if not already present
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def read_delta_table(table_name: str) -> DataFrame:
    """
    Read data from a Delta table.
    
    Args:
        table_name: The name of the Delta table
        
    Returns:
        DataFrame: The data from the Delta table
    """
    spark = get_spark_session()
    try:
        return spark.read.format("delta").table(table_name)
    except Exception as e:
        logger = setup_logger("read_delta_table")
        logger.error(f"Error reading Delta table {table_name}: {str(e)}")
        raise

def write_to_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Write data to a Delta table.
    
    Args:
        df: The DataFrame to write
        table_name: The name of the Delta table
        mode: The write mode (default: overwrite)
    """
    logger = setup_logger("write_to_delta_table")
    
    try:
        if df.rdd.isEmpty():
            logger.warning(f"DataFrame is empty. Not writing to {table_name}.")
            return
            
        df.write.format("delta").mode(mode).saveAsTable(table_name)
        logger.info(f"Successfully wrote data to {table_name} in {mode} mode.")
    except Exception as e:
        logger.error(f"Error writing to Delta table {table_name}: {str(e)}")
        raise

def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Add audit columns to a DataFrame.
    
    Args:
        df: The input DataFrame
        
    Returns:
        DataFrame: The DataFrame with audit columns
    """
    spark = get_spark_session()
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return df.withColumn("created_at", spark.sql(f"'{current_timestamp}'")) \
             .withColumn("updated_at", spark.sql(f"'{current_timestamp}'")) \
             .withColumn("is_active", spark.sql("true"))

def handle_null_data(df: DataFrame, required_columns: List[str] = None) -> DataFrame:
    """
    Handle null data in a DataFrame.
    
    Args:
        df: The input DataFrame
        required_columns: List of columns that must not be null
        
    Returns:
        DataFrame: The DataFrame with null handling
    """
    if required_columns:
        return df.dropna(subset=required_columns)
    return df.na.drop()

def validate_dataframe(df: DataFrame, expected_columns: List[str]) -> bool:
    """
    Validate that a DataFrame contains the expected columns.
    
    Args:
        df: The DataFrame to validate
        expected_columns: List of expected column names
        
    Returns:
        bool: True if the DataFrame contains all expected columns, False otherwise
    """
    actual_columns = set(df.columns)
    missing_columns = set(expected_columns) - actual_columns
    
    if missing_columns:
        logger = setup_logger("validate_dataframe")
        logger.error(f"Missing columns in DataFrame: {missing_columns}")
        return False
    
    return True