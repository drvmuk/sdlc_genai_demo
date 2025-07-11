"""
Module to clean customer and order data by removing null and duplicate records.
Implementation of TR-DLT-003.
"""
from pyspark.sql.functions import col
from utils import get_spark_session, setup_logger, get_delta_table_path
from config import TARGET_CATALOG, TARGET_SCHEMA, CUSTOMER_TABLE, ORDER_TABLE

logger = setup_logger("clean_data")

def clean_table_data(spark, catalog, schema, table_name):
    """
    Remove null and duplicate records from a Delta table.
    
    Args:
        spark: SparkSession
        catalog (str): The catalog name
        schema (str): The schema name
        table_name (str): The table name
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the table path
        table_path = get_delta_table_path(catalog, schema, table_name)
        
        logger.info(f"Reading data from {table_path}")
        
        # Read the data
        df = spark.read.format("delta").table(table_path)
        
        # Get primary key column (assuming first column is the primary key)
        primary_key = df.columns[0]
        
        logger.info(f"Removing null values and duplicates using primary key: {primary_key}")
        
        # Remove rows with null values in any column
        cleaned_df = df.dropna()
        
        # Remove duplicate records based on primary key
        cleaned_df = cleaned_df.dropDuplicates([primary_key])
        
        # Get counts for logging
        original_count = df.count()
        cleaned_count = cleaned_df.count()
        removed_count = original_count - cleaned_count
        
        logger.info(f"Removed {removed_count} records from {table_name}")
        
        # Write back to the table
        logger.info(f"Writing cleaned data back to {table_path}")
        cleaned_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_path)
        
        logger.info(f"Successfully cleaned {table_name} data")
        return True
    
    except Exception as e:
        logger.error(f"Error cleaning {table_name} data: {str(e)}")
        return False

def main():
    """Main function to clean customer and order data."""
    spark = get_spark_session()
    
    # Clean customer data
    customer_success = clean_table_data(
        spark, 
        TARGET_CATALOG, 
        TARGET_SCHEMA, 
        CUSTOMER_TABLE
    )
    
    # Clean order data
    order_success = clean_table_data(
        spark, 
        TARGET_CATALOG, 
        TARGET_SCHEMA, 
        ORDER_TABLE
    )
    
    if customer_success and order_success:
        logger.info("Data cleaning completed successfully")
    else:
        logger.error("Data cleaning failed")

if __name__ == "__main__":
    main()