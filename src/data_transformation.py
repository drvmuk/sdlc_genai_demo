"""
Data transformation and enrichment module.

This module implements TR-DP-002: Data Transformation and Enrichment.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, sum as spark_sum, when

from config import (
    CLEANSED_CUSTOMER_DATA_PATH, CLEANSED_ORDER_DATA_PATH,
    PROCESSED_DATA_PATH, AGGREGATED_DATA_PATH,
    EFFECTIVE_FROM_COL, EFFECTIVE_TO_COL, IS_CURRENT_COL
)
from utils import setup_logger, apply_scd_type2

def load_cleansed_data(spark, logger):
    """
    Load cleansed customer and order data.
    
    Args:
        spark (SparkSession): Spark session
        logger (logging.Logger): Logger instance
        
    Returns:
        tuple: (customer_df, order_df)
    """
    logger.info(f"Loading cleansed customer data from {CLEANSED_CUSTOMER_DATA_PATH}")
    customer_df = spark.read.parquet(CLEANSED_CUSTOMER_DATA_PATH)
    
    logger.info(f"Loading cleansed order data from {CLEANSED_ORDER_DATA_PATH}")
    order_df = spark.read.parquet(CLEANSED_ORDER_DATA_PATH)
    
    return customer_df, order_df

def join_customer_order_data(customer_df, order_df, logger):
    """
    Join customer and order data on CustId.
    
    Args:
        customer_df (DataFrame): Customer DataFrame
        order_df (DataFrame): Order DataFrame
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: Joined DataFrame
    """
    logger.info("Joining customer and order data on CustId")
    
    # Rename columns to avoid ambiguity
    customer_df = customer_df.select([col(c).alias(f"cust_{c}") for c in customer_df.columns])
    order_df = order_df.select([col(c).alias(f"order_{c}") for c in order_df.columns])
    
    # Join on CustId
    joined_df = customer_df.join(
        order_df,
        customer_df["cust_CustId"] == order_df["order_CustId"],
        "inner"
    )
    
    logger.info(f"Joined data has {joined_df.count()} records")
    return joined_df

def apply_scd_type2_to_customer_data(customer_df, logger):
    """
    Apply SCD Type 2 logic to customer data.
    
    Args:
        customer_df (DataFrame): Customer DataFrame
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: Customer DataFrame with SCD Type 2 columns
    """
    logger.info("Applying SCD Type 2 logic to customer data")
    
    # Add SCD Type 2 columns if they don't exist
    if EFFECTIVE_FROM_COL not in customer_df.columns:
        customer_df = customer_df.withColumn(EFFECTIVE_FROM_COL, current_timestamp())
    
    if EFFECTIVE_TO_COL not in customer_df.columns:
        customer_df = customer_df.withColumn(EFFECTIVE_TO_COL, lit(None))
    
    if IS_CURRENT_COL not in customer_df.columns:
        customer_df = customer_df.withColumn(IS_CURRENT_COL, lit(True))
    
    logger.info("SCD Type 2 logic applied to customer data")
    return customer_df

def calculate_total_amount(joined_df, logger):
    """
    Calculate TotalAmount by multiplying PricePerUnit and Qty.
    
    Args:
        joined_df (DataFrame): Joined DataFrame
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: DataFrame with TotalAmount column
    """
    logger.info("Calculating TotalAmount")
    
    result_df = joined_df.withColumn(
        "TotalAmount",
        col("order_PricePerUnit") * col("order_Qty")
    )
    
    logger.info("TotalAmount calculation completed")
    return result_df

def aggregate_by_name_and_date(result_df, logger):
    """
    Aggregate data by Name and Date.
    
    Args:
        result_df (DataFrame): Result DataFrame
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: Aggregated DataFrame
    """
    logger.info("Aggregating data by Name and Date")
    
    aggregated_df = result_df.groupBy("cust_Name", "order_Date") \
        .agg(
            spark_sum("TotalAmount").alias("TotalSpend"),
            spark_sum("order_Qty").alias("TotalQuantity")
        )
    
    logger.info(f"Aggregation completed. {aggregated_df.count()} aggregated records")
    return aggregated_df

def save_processed_data(result_df, aggregated_df, logger):
    """
    Save processed and aggregated data.
    
    Args:
        result_df (DataFrame): Result DataFrame
        aggregated_df (DataFrame): Aggregated DataFrame
        logger (logging.Logger): Logger instance
    """
    logger.info(f"Saving processed data to {PROCESSED_DATA_PATH}")
    result_df.write.mode("overwrite").parquet(PROCESSED_DATA_PATH)
    
    logger.info(f"Saving aggregated data to {AGGREGATED_DATA_PATH}")
    aggregated_df.write.mode("overwrite").parquet(AGGREGATED_DATA_PATH)
    
    logger.info("Processed and aggregated data saved successfully")

def main():
    """
    Main function to execute the data transformation and enrichment job.
    """
    spark = SparkSession.builder \
        .appName("Data Transformation and Enrichment") \
        .getOrCreate()
    
    logger = setup_logger("data_transformation")
    logger.info("Starting data transformation and enrichment job")
    
    try:
        # Step 1: Load cleansed data
        customer_df, order_df = load_cleansed_data(spark, logger)
        
        # Step 2: Apply SCD Type 2 logic to customer data
        customer_df = apply_scd_type2_to_customer_data(customer_df, logger)
        
        # Step 3: Join customer and order data
        joined_df = join_customer_order_data(customer_df, order_df, logger)
        
        # Step 4: Calculate TotalAmount
        result_df = calculate_total_amount(joined_df, logger)
        
        # Step 5: Aggregate data by Name and Date
        aggregated_df = aggregate_by_name_and_date(result_df, logger)
        
        # Step 6: Save processed and aggregated data
        save_processed_data(result_df, aggregated_df, logger)
        
        logger.info("Data transformation and enrichment job completed successfully")
    except Exception as e:
        logger.error(f"Error in data transformation and enrichment job: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()