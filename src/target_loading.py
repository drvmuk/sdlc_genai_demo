"""
Target table creation and data loading module.

This module implements TR-DP-003: Target Table Creation and Data Loading.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import (
    PROCESSED_DATA_PATH, AGGREGATED_DATA_PATH,
    ORDER_SUMMARY_PATH, CUSTOMER_AGGREGATE_SPEND_PATH
)
from utils import setup_logger

def load_processed_data(spark, logger):
    """
    Load processed and aggregated data.
    
    Args:
        spark (SparkSession): Spark session
        logger (logging.Logger): Logger instance
        
    Returns:
        tuple: (processed_df, aggregated_df)
    """
    logger.info(f"Loading processed data from {PROCESSED_DATA_PATH}")
    processed_df = spark.read.parquet(PROCESSED_DATA_PATH)
    
    logger.info(f"Loading aggregated data from {AGGREGATED_DATA_PATH}")
    aggregated_df = spark.read.parquet(AGGREGATED_DATA_PATH)
    
    return processed_df, aggregated_df

def create_order_summary_table(spark, logger):
    """
    Create ordersummary table if it does not exist.
    
    Args:
        spark (SparkSession): Spark session
        logger (logging.Logger): Logger instance
    """
    logger.info("Creating ordersummary table if it does not exist")
    
    try:
        # Check if table exists
        tables = spark.catalog.listTables()
        table_exists = any(table.name == "ordersummary" for table in tables)
        
        if not table_exists:
            # Create table
            spark.sql("""
                CREATE TABLE IF NOT EXISTS ordersummary (
                    cust_CustId STRING,
                    cust_Name STRING,
                    cust_Address STRING,
                    order_OrderId STRING,
                    order_CustId STRING,
                    order_PricePerUnit DOUBLE,
                    order_Qty INT,
                    order_Date DATE,
                    TotalAmount DOUBLE,
                    EffectiveFrom TIMESTAMP,
                    EffectiveTo TIMESTAMP,
                    IsCurrent BOOLEAN
                )
                USING PARQUET
                LOCATION '/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/ordersummary'
            """)
            logger.info("ordersummary table created successfully")
        else:
            logger.info("ordersummary table already exists")
    except Exception as e:
        logger.error(f"Error creating ordersummary table: {str(e)}")
        raise

def create_customer_aggregate_spend_table(spark, logger):
    """
    Create customeraggregatespend table if it does not exist.
    
    Args:
        spark (SparkSession): Spark session
        logger (logging.Logger): Logger instance
    """
    logger.info("Creating customeraggregatespend table if it does not exist")
    
    try:
        # Check if table exists
        tables = spark.catalog.listTables()
        table_exists = any(table.name == "customeraggregatespend" for table in tables)
        
        if not table_exists:
            # Create table
            spark.sql("""
                CREATE TABLE IF NOT EXISTS customeraggregatespend (
                    cust_Name STRING,
                    order_Date DATE,
                    TotalSpend DOUBLE,
                    TotalQuantity BIGINT
                )
                USING PARQUET
                LOCATION '/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customeraggregatespend'
            """)
            logger.info("customeraggregatespend table created successfully")
        else:
            logger.info("customeraggregatespend table already exists")
    except Exception as e:
        logger.error(f"Error creating customeraggregatespend table: {str(e)}")
        raise

def load_data_to_order_summary(processed_df, logger):
    """
    Load processed data into ordersummary table.
    
    Args:
        processed_df (DataFrame): Processed DataFrame
        logger (logging.Logger): Logger instance
    """
    logger.info(f"Loading data into ordersummary table at {ORDER_SUMMARY_PATH}")
    
    try:
        processed_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(ORDER_SUMMARY_PATH)
        
        logger.info(f"Successfully loaded {processed_df.count()} records into ordersummary table")
    except Exception as e:
        logger.error(f"Error loading data into ordersummary table: {str(e)}")
        raise

def load_data_to_customer_aggregate_spend(aggregated_df, logger):
    """
    Load aggregated data into customeraggregatespend table.
    
    Args:
        aggregated_df (DataFrame): Aggregated DataFrame
        logger (logging.Logger): Logger instance
    """
    logger.info(f"Loading data into customeraggregatespend table at {CUSTOMER_AGGREGATE_SPEND_PATH}")
    
    try:
        aggregated_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(CUSTOMER_AGGREGATE_SPEND_PATH)
        
        logger.info(f"Successfully loaded {aggregated_df.count()} records into customeraggregatespend table")
    except Exception as e:
        logger.error(f"Error loading data into customeraggregatespend table: {str(e)}")
        raise

def main():
    """
    Main function to execute the target table creation and data loading job.
    """
    spark = SparkSession.builder \
        .appName("Target Table Creation and Data Loading") \
        .getOrCreate()
    
    logger = setup_logger("target_loading")
    logger.info("Starting target table creation and data loading job")
    
    try:
        # Step 1: Load processed and aggregated data
        processed_df, aggregated_df = loa