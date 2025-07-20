"""
Module for cleansing customer and order data by removing null and duplicate records.
Implements TR-ORD-002.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.config import CUSTOMER_DELTA_TABLE, ORDER_DELTA_TABLE
from src.logger import get_logger, log_job_start, log_job_end

logger = get_logger(__name__)

def create_spark_session():
    """Create and return a SparkSession"""
    return SparkSession.builder \
        .appName("Data Cleaner") \
        .getOrCreate()

def cleanse_table(spark, table_name):
    """
    Remove null and duplicate records from a Delta table
    
    Args:
        spark: SparkSession
        table_name: Name of the Delta table to cleanse
        
    Returns:
        tuple: (rows_before, rows_after) count of records before and after cleansing
    """
    logger.info(f"Cleansing table: {table_name}")
    
    # Read the table
    df = spark.read.format("delta").table(table_name)
    rows_before = df.count()
    logger.info(f"Initial record count: {rows_before}")
    
    # Get column names for the table
    columns = df.columns
    
    # Remove records with null values in any column
    for column in columns:
        df = df.filter(col(column).isNotNull())
    
    # Remove duplicate records
    df = df.dropDuplicates()
    
    # Write back to the table
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    
    rows_after = df.count()
    logger.info(f"Final record count: {rows_after}")
    logger.info(f"Removed {rows_before - rows_after} records")
    
    return rows_before, rows_after

def cleanse_customer_data(spark):
    """Cleanse customer data by removing null and duplicate records"""
    logger.info("Cleansing customer data")
    return cleanse_table(spark, CUSTOMER_DELTA_TABLE)

def cleanse_order_data(spark):
    """Cleanse order data by removing null and duplicate records"""
    logger.info("Cleansing order data")
    return cleanse_table(spark, ORDER_DELTA_TABLE)

def main():
    """Main function to cleanse customer and order data"""
    job_name = "Customer and Order Data Cleansing"
    log_job_start(logger, job_name)
    
    try:
        spark = create_spark_session()
        cleanse_customer_data(spark)
        cleanse_order_data(spark)
        log_job_end(logger, job_name)
        
    except Exception as e:
        logger.error(f"Error in {job_name}: {str(e)}")
        raise

if __name__ == "__main__":
    main()