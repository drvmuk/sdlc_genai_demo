"""
Utility functions for the Databricks Delta ETL pipeline.
"""
from pyspark.sql import SparkSession
import logging

def get_spark_session():
    """
    Get or create a SparkSession with Delta Lake support.
    
    Returns:
        SparkSession: The active Spark session
    """
    return (SparkSession.builder
            .appName("Databricks Delta ETL")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def setup_logger(name):
    """
    Set up a logger with the specified name.
    
    Args:
        name (str): The name for the logger
        
    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create console handler if not already present
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def get_delta_table_path(catalog, schema, table):
    """
    Get the full path to a Delta table.
    
    Args:
        catalog (str): The catalog name
        schema (str): The schema name
        table (str): The table name
        
    Returns:
        str: The full table path
    """
    return f"{catalog}.{schema}.{table}"