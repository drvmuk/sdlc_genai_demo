"""
Module to create an ordersummary table and load joined data from customer and order tables.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
import logging
from datetime import datetime
from src.config import CUSTOMER_DELTA_TABLE, ORDER_DELTA_TABLE, ORDERSUMMARY_DELTA_TABLE
from src.utils import setup_logger, read_delta_table, write_to_delta_table

logger = setup_logger("create_ordersummary")

def join_customer_order_data(customer_df: DataFrame, order_df: DataFrame) -> DataFrame:
    """
    Join customer and order data using CustId as the join key.
    
    Args:
        customer_df: The customer DataFrame
        order_df: The order DataFrame
        
    Returns:
        DataFrame: The joined DataFrame
    """
    try:
        # Validate that CustId exists in both DataFrames
        if "CustId" not in customer_df.columns:
            raise ValueError("CustId column not found in customer data")
        if "CustId" not in order_df.columns:
            raise ValueError("CustId column not found in order data")
        
        # Join customer and order data
        joined_df = customer_df.join(order_df, "CustId", "inner")
        
        logger.info(f"Successfully joined customer and order data. Result has {joined_df.count()} rows.")
        return joined_df
    except Exception as e:
        logger.error(f"Error joining customer and order data: {str(e)}")
        raise

def add_scd_type2_columns(df: DataFrame) -> DataFrame:
    """
    Add SCD Type 2 columns to the DataFrame.
    
    Args:
        df: The input DataFrame
        
    Returns:
        DataFrame: The DataFrame with SCD Type 2 columns
    """
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Add SCD Type 2 columns
        scd_df = df.withColumn("StartDate", lit(current_date)) \
                   .withColumn("EndDate", lit("9999-12-31")) \
                   .withColumn("IsActive", lit(True)) \
                   .withColumn("LastUpdated", current_timestamp())
        
        logger.info("Successfully added SCD Type 2 columns")
        return scd_df
    except Exception as e:
        logger.error(f"Error adding SCD Type 2 columns: {str(e)}")
        raise

def create_ordersummary_table(spark: SparkSession, joined_df: DataFrame) -> None:
    """
    Create ordersummary table and load joined data.
    
    Args:
        spark: The SparkSession
        joined_df: The joined DataFrame
    """
    try:
        # Check if the table exists
        tables = spark.sql("SHOW TABLES IN gen_ai_poc_databrickscoe.sdlc_wizard").collect()
        table_exists = any(row.tableName == "ordersummary" for row in tables)
        
        # Add SCD Type 2 columns
        ordersummary_df = add_scd_type2_columns(joined_df)
        
        # Write to ordersummary table
        if table_exists:
            logger.info("ordersummary table already exists. Appending data.")
            write_to_delta_table(ordersummary_df, ORDERSUMMARY_DELTA_TABLE, mode="append")
        else:
            logger.info("Creating ordersummary table.")
            write_to_delta_table(ordersummary_df, ORDERSUMMARY_DELTA_TABLE, mode="overwrite")
        
        logger.info(f"Successfully created/updated ordersummary table with {ordersummary_df.count()} rows.")
    except Exception as e:
        logger.error(f"Error creating ordersummary table: {str(e)}")
        raise

def main():
    """
    Main function to create ordersummary table and load joined data.
    """
    try:
        spark = SparkSession.builder.appName("Create OrderSummary Table").getOrCreate()
        
        # Read customer data from Delta table
        logger.info(f"Reading customer data from {CUSTOMER_DELTA_TABLE}")
        customer_df = read_delta_table(CUSTOMER_DELTA_TABLE)
        
        # Read order data from Delta table
        logger.info(f"Reading order data from {ORDER_DELTA_TABLE}")
        order_df = read_delta_table(ORDER_DELTA_TABLE)
        
        # Basic validation
        if customer_df.count() == 0:
            logger.warning("Customer data is empty")
            return
        if order_df.count() == 0:
            logger.warning("Order data is empty")
            return
        
        # Join customer and order data
        joined_df = join_customer_order_data(customer_df, order_df)
        
        # Create ordersummary table and load joined data
        create_ordersummary_table(spark, joined_df)
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise
    finally:
        logger.info("OrderSummary table creation process completed")

if __name__ == "__main__":
    main()