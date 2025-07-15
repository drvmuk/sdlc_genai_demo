"""
Module to load source CSV data into Delta tables (TR-DELTA-001)
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from src.config import (
    CUSTOMER_CSV_PATH,
    ORDER_CSV_PATH,
    CUSTOMER_DELTA_TABLE,
    ORDER_DELTA_TABLE
)
from src.utils import log_info, log_error

def load_csv_to_delta():
    """
    Load customer and order CSV data into Delta tables
    """
    spark = SparkSession.builder.appName("LoadCsvToDelta").getOrCreate()
    
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Load customer data
        log_info(f"Reading customer data from {CUSTOMER_CSV_PATH}")
        try:
            customer_df = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv(CUSTOMER_CSV_PATH)
            
            # Add metadata columns
            customer_df = customer_df.withColumn("ingestion_timestamp", current_timestamp())
            
            log_info(f"Writing customer data to Delta table {CUSTOMER_DELTA_TABLE}")
            customer_df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(CUSTOMER_DELTA_TABLE)
            
            log_info(f"Successfully loaded customer data into {CUSTOMER_DELTA_TABLE}")
        except Exception as e:
            log_error("Error loading customer data", e)
            raise
        
        # Load order data
        log_info(f"Reading order data from {ORDER_CSV_PATH}")
        try:
            order_df = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv(ORDER_CSV_PATH)
            
            # Add metadata columns
            order_df = order_df.withColumn("ingestion_timestamp", current_timestamp())
            
            log_info(f"Writing order data to Delta table {ORDER_DELTA_TABLE}")
            order_df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(ORDER_DELTA_TABLE)
            
            log_info(f"Successfully loaded order data into {ORDER_DELTA_TABLE}")
        except Exception as e:
            log_error("Error loading order data", e)
            raise
            
    except Exception as e:
        log_error("Error in load_csv_to_delta", e)
        raise

if __name__ == "__main__":
    load_csv_to_delta()