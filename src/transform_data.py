"""
Module to transform order data by adding TotalAmount column.
Implementation of TR-DLT-002.
"""
from pyspark.sql.functions import col, round
from utils import get_spark_session, setup_logger, get_delta_table_path
from config import TARGET_CATALOG, TARGET_SCHEMA, ORDER_TABLE

logger = setup_logger("transform_data")

def add_total_amount_column(spark):
    """
    Add TotalAmount column to order data by multiplying PricePerUnit and Qty.
    
    Args:
        spark: SparkSession
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the order table path
        order_table_path = get_delta_table_path(TARGET_CATALOG, TARGET_SCHEMA, ORDER_TABLE)
        
        logger.info(f"Reading order data from {order_table_path}")
        
        # Read the order data
        order_df = spark.read.format("delta").table(order_table_path)
        
        # Check if required columns exist
        required_columns = ["PricePerUnit", "Qty"]
        missing_columns = [col for col in required_columns if col not in order_df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        logger.info("Adding TotalAmount column")
        
        # Add TotalAmount column by multiplying PricePerUnit and Qty
        transformed_df = order_df.withColumn(
            "TotalAmount", 
            round(col("PricePerUnit") * col("Qty"), 2)
        )
        
        # Write back to the order table
        logger.info(f"Writing transformed data back to {order_table_path}")
        transformed_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(order_table_path)
        
        logger.info("Successfully added TotalAmount column to order data")
        return True
    
    except Exception as e:
        logger.error(f"Error transforming order data: {str(e)}")
        return False

def main():
    """Main function to transform order data."""
    spark = get_spark_session()
    
    success = add_total_amount_column(spark)
    
    if success:
        logger.info("Order data transformation completed successfully")
    else:
        logger.error("Order data transformation failed")

if __name__ == "__main__":
    main()