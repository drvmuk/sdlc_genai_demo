"""
Module to transform order data by adding a TotalAmount column.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, round
import logging
from src.config import ORDER_DELTA_TABLE
from src.utils import setup_logger, read_delta_table, write_to_delta_table

logger = setup_logger("transform_order_data")

def add_total_amount_column(order_df: DataFrame) -> DataFrame:
    """
    Add a TotalAmount column to the order DataFrame.
    
    Args:
        order_df: The order DataFrame
        
    Returns:
        DataFrame: The order DataFrame with TotalAmount column
    """
    try:
        # Validate that required columns exist
        required_columns = ["PricePerUnit", "Qty"]
        for col_name in required_columns:
            if col_name not in order_df.columns:
                raise ValueError(f"Required column '{col_name}' not found in order data")
        
        # Add TotalAmount column as PricePerUnit * Qty
        transformed_df = order_df.withColumn("TotalAmount", 
                                            round(col("PricePerUnit") * col("Qty"), 2))
        
        logger.info("Successfully added TotalAmount column to order data")
        return transformed_df
    except Exception as e:
        logger.error(f"Error adding TotalAmount column: {str(e)}")
        raise

def main():
    """
    Main function to transform order data.
    """
    try:
        spark = SparkSession.builder.appName("Transform Order Data").getOrCreate()
        
        # Read order data from Delta table
        logger.info(f"Reading order data from {ORDER_DELTA_TABLE}")
        order_df = read_delta_table(ORDER_DELTA_TABLE)
        
        # Basic validation
        if order_df.count() == 0:
            logger.warning("Order data is empty")
            return
        
        # Add TotalAmount column
        transformed_df = add_total_amount_column(order_df)
        
        # Write transformed data back to Delta table
        write_to_delta_table(transformed_df, ORDER_DELTA_TABLE)
        logger.info(f"Successfully wrote transformed order data to {ORDER_DELTA_TABLE}")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise
    finally:
        logger.info("Order data transformation completed")

if __name__ == "__main__":
    main()