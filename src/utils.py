"""
Utility functions for the Delta data processing pipeline
"""
import logging
from pyspark.sql import SparkSession

def get_spark_session():
    """
    Get or create a SparkSession
    
    Returns:
        SparkSession: The active SparkSession
    """
    return SparkSession.builder.appName("DeltaDataProcessing").getOrCreate()

def table_exists(spark, table_name):
    """
    Check if a table exists in the catalog
    
    Args:
        spark: SparkSession
        table_name: Fully qualified table name
        
    Returns:
        bool: True if the table exists, False otherwise
    """
    try:
        parts = table_name.split('.')
        if len(parts) == 3:
            catalog, schema, table = parts
            tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
            return any(row.tableName == table for row in tables)
        else:
            schema, table = parts
            tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
            return any(row.tableName == table for row in tables)
    except Exception as e:
        logging.error(f"Error checking if table {table_name} exists: {str(e)}")
        return False

def log_info(message):
    """Log an info message"""
    logging.info(message)

def log_error(message, exception=None):
    """Log an error message with optional exception details"""
    if exception:
        logging.error(f"{message}: {str(exception)}")
    else:
        logging.error(message)