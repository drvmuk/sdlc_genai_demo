"""
Module to load customer and order data from CSV files into Delta tables.
Implementation of TR-DLT-001.
"""
from pyspark.sql.functions import col
from utils import get_spark_session, setup_logger, get_delta_table_path
from config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH, 
    TARGET_CATALOG, TARGET_SCHEMA, 
    CUSTOMER_TABLE, ORDER_TABLE
)

logger = setup_logger("load_data")

def load_csv_to_delta(spark, source_path, catalog, schema, table_name):
    """
    Load CSV data into a Delta table.
    
    Args:
        spark: SparkSession
        source_path (str): Path to source CSV data
        catalog (str): Target catalog
        schema (str): Target schema
        table_name (str): Target table name
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Reading CSV data from {source_path}")
        df = spark.read.option("header", "true") \
                      .option("inferSchema", "true") \
                      .csv(source_path)
        
        logger.info(f"CSV data schema: {df.schema}")
        
        # Create target table path
        target_table = get_delta_table_path(catalog, schema, table_name)
        
        logger.info(f"Writing data to Delta table: {target_table}")
        
        # Write to Delta table
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .saveAsTable(target_table)
        
        logger.info(f"Successfully loaded data into {target_table}")
        return True
    
    except Exception as e:
        logger.error(f"Error loading data into Delta table: {str(e)}")
        return False

def main():
    """Main function to load customer and order data into Delta tables."""
    spark = get_spark_session()
    
    # Create schema if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
    
    # Load customer data
    customer_success = load_csv_to_delta(
        spark, 
        CUSTOMER_DATA_PATH, 
        TARGET_CATALOG, 
        TARGET_SCHEMA, 
        CUSTOMER_TABLE
    )
    
    # Load order data
    order_success = load_csv_to_delta(
        spark, 
        ORDER_DATA_PATH, 
        TARGET_CATALOG, 
        TARGET_SCHEMA, 
        ORDER_TABLE
    )
    
    if customer_success and order_success:
        logger.info("Data loading completed successfully")
    else:
        logger.error("Data loading failed")

if __name__ == "__main__":
    main()