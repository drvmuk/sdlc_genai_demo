"""
Module to create ordersummary table by joining customer and order data.
Implementation of TR-DLT-004.
"""
from pyspark.sql.functions import current_timestamp, lit
from utils import get_spark_session, setup_logger, get_delta_table_path
from config import TARGET_CATALOG, TARGET_SCHEMA, CUSTOMER_TABLE, ORDER_TABLE, ORDER_SUMMARY_TABLE

logger = setup_logger("create_order_summary")

def create_order_summary(spark):
    """
    Create ordersummary table by joining customer and order data.
    
    Args:
        spark: SparkSession
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get table paths
        customer_table_path = get_delta_table_path(TARGET_CATALOG, TARGET_SCHEMA, CUSTOMER_TABLE)
        order_table_path = get_delta_table_path(TARGET_CATALOG, TARGET_SCHEMA, ORDER_TABLE)
        order_summary_table_path = get_delta_table_path(TARGET_CATALOG, TARGET_SCHEMA, ORDER_SUMMARY_TABLE)
        
        logger.info(f"Reading customer data from {customer_table_path}")
        logger.info(f"Reading order data from {order_table_path}")
        
        # Read the customer and order data
        customer_df = spark.read.format("delta").table(customer_table_path)
        order_df = spark.read.format("delta").table(order_table_path)
        
        # Check if required join column exists
        if "CustId" not in customer_df.columns or "CustId" not in order_df.columns:
            logger.error("Missing required join column 'CustId'")
            return False
        
        logger.info("Joining customer and order data on CustId")
        
        # Join customer and order data on CustId
        joined_df = customer_df.join(
            order_df,
            customer_df.CustId == order_df.CustId,
            "inner"
        ).drop(order_df.CustId)  # Remove duplicate CustId column
        
        # Add SCD Type 2 columns
        joined_df = joined_df.withColumn("IsActive", lit(True)) \
                            .withColumn("StartDate", current_timestamp()) \
                            .withColumn("EndDate", lit(None).cast("timestamp"))
        
        # Write to ordersummary table
        logger.info(f"Writing joined data to {order_summary_table_path}")
        joined_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(order_summary_table_path)
        
        logger.info("Successfully created ordersummary table")
        return True
    
    except Exception as e:
        logger.error(f"Error creating ordersummary table: {str(e)}")
        return False

def main():
    """Main function to create ordersummary table."""
    spark = get_spark_session()
    
    success = create_order_summary(spark)
    
    if success:
        logger.info("Order summary creation completed successfully")
    else:
        logger.error("Order summary creation failed")

if __name__ == "__main__":
    main()