"""
Module for managing database tables.
"""
from pyspark.sql import SparkSession

from src.config import DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FULL_TABLE_NAME, ORDERSUMMARY_SCHEMA
from src.logging_utils import log_info, log_error, log_start_job, log_end_job

def get_spark_session() -> SparkSession:
    """
    Get or create a Spark session.
    
    Returns:
        SparkSession: Active Spark session
    """
    return SparkSession.builder.getOrCreate()

def check_table_exists(table_name: str) -> bool:
    """
    Check if a table exists.
    
    Args:
        table_name: Full table name (database.schema.table)
        
    Returns:
        bool: True if table exists, False otherwise
    """
    spark = get_spark_session()
    
    try:
        tables = spark.sql(f"SHOW TABLES IN {DATABASE_NAME}.{SCHEMA_NAME}").collect()
        table_exists = any(row.tableName == TABLE_NAME for row in tables)
        
        if table_exists:
            log_info(f"Table {table_name} exists")
        else:
            log_info(f"Table {table_name} does not exist")
            
        return table_exists
    
    except Exception as e:
        log_error(f"Error checking if table {table_name} exists", e)
        # If we can't check, assume it doesn't exist
        return False

def create_database_and_schema():
    """
    Create database and schema if they don't exist.
    """
    spark = get_spark_session()
    
    try:
        # Create database if it doesn't exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        log_info(f"Database {DATABASE_NAME} created or already exists")
        
        # Create schema if it doesn't exist
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME}")
        log_info(f"Schema {DATABASE_NAME}.{SCHEMA_NAME} created or already exists")
    
    except Exception as e:
        log_error("Error creating database and schema", e)
        raise

def create_ordersummary_table():
    """
    Create ordersummary table if it doesn't exist.
    """
    log_start_job("Create ordersummary Table")
    spark = get_spark_session()
    
    try:
        # Create database and schema if they don't exist
        create_database_and_schema()
        
        # Check if table exists
        if not check_table_exists(FULL_TABLE_NAME):
            # Create table
            create_table_sql = f"""
            CREATE TABLE {FULL_TABLE_NAME} (
                {ORDERSUMMARY_SCHEMA}
            )
            USING DELTA
            """
            
            spark.sql(create_table_sql)
            log_info(f"Table {FULL_TABLE_NAME} created successfully")
        
        log_end_job("Create ordersummary Table")
    
    except Exception as e:
        log_error(f"Error creating table {FULL_TABLE_NAME}", e)
        raise