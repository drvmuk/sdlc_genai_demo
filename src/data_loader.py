"""
Module for loading customer and order data from CSV to Delta tables.
Implements TR-ORD-001.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import time

from src.config import CUSTOMER_CSV_PATH, ORDER_CSV_PATH, CUSTOMER_DELTA_TABLE, ORDER_DELTA_TABLE
from src.logger import get_logger, log_job_start, log_job_end

logger = get_logger(__name__)

def create_spark_session():
    """Create and return a SparkSession"""
    return SparkSession.builder \
        .appName("Data Loader") \
        .getOrCreate()

def load_csv_to_delta(spark, csv_path, delta_table, max_retries=3):
    """
    Load data from CSV to Delta table with retry mechanism
    
    Args:
        spark: SparkSession
        csv_path: Path to CSV data
        delta_table: Target Delta table name
        max_retries: Maximum number of retry attempts
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"Reading CSV data from {csv_path}")
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
            
            # Add metadata columns
            df = df.withColumn("ingestion_timestamp", current_timestamp())
            
            logger.info(f"Writing data to Delta table {delta_table}")
            df.write.format("delta").mode("overwrite").saveAsTable(delta_table)
            
            logger.info(f"Successfully loaded data to {delta_table}")
            return True
            
        except Exception as e:
            retry_count += 1
            logger.error(f"Error loading data to {delta_table}: {str(e)}")
            logger.info(f"Retry attempt {retry_count} of {max_retries}")
            
            if retry_count >= max_retries:
                logger.error(f"Failed to load data after {max_retries} attempts")
                raise
            
            # Exponential backoff
            time.sleep(2 ** retry_count)
    
    return False

def load_customer_data(spark):
    """Load customer data from CSV to Delta table"""
    logger.info("Loading customer data")
    return load_csv_to_delta(spark, CUSTOMER_CSV_PATH, CUSTOMER_DELTA_TABLE)

def load_order_data(spark):
    """Load order data from CSV to Delta table"""
    logger.info("Loading order data")
    return load_csv_to_delta(spark, ORDER_CSV_PATH, ORDER_DELTA_TABLE)

def main():
    """Main function to load customer and order data"""
    job_name = "Customer and Order Data Loading"
    log_job_start(logger, job_name)
    
    try:
        spark = create_spark_session()
        load_customer_data(spark)
        load_order_data(spark)
        log_job_end(logger, job_name)
        
    except Exception as e:
        logger.error(f"Error in {job_name}: {str(e)}")
        raise

if __name__ == "__main__":
    main()