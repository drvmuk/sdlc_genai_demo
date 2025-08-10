"""
Utility functions for the ETL pipeline
"""
from pyspark.sql import SparkSession
import logging

def get_spark_session():
    """
    Get or create a Spark session
    
    Returns:
        SparkSession: The current Spark session
    """
    return SparkSession.builder.getOrCreate()

def table_exists(table_name):
    """
    Check if a table exists
    
    Args:
        table_name (str): The name of the table to check
        
    Returns:
        bool: True if the table exists, False otherwise
    """
    spark = get_spark_session()
    tables = spark.sql(f"SHOW TABLES IN {table_name.split('.')[0]}.{table_name.split('.')[1]}")
    return tables.filter(tables.tableName == table_name.split('.')[-1]).count() > 0

def log_info(message):
    """
    Log an info message
    
    Args:
        message (str): The message to log
    """
    logging.info(message)

def log_error(message, exception=None):
    """
    Log an error message
    
    Args:
        message (str): The message to log
        exception (Exception, optional): The exception to log
    """
    if exception:
        logging.error(f"{message}: {str(exception)}")
    else:
        logging.error(message)