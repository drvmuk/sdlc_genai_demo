"""
Module to create customeraggregatespend table with aggregated data.
Implementation of TR-DLT-006.
"""
from pyspark.sql.functions import col, sum as spark_sum, date_format
from utils import get_spark_session, setup_logger, get_delta_table_path
from config import TARGET_CATALOG, TARGET_SCHEMA, ORDER_SUMMARY_TABLE, CUSTOMER_AGGREGATE_TABLE

logger = setup_logger("create_customer_aggregate")

def create_customer_aggregate_spend(spark):
    """
    Create customeraggregatespend table with aggregated data.
    
    Args:
        spark: SparkSession
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get table paths
        order_summary_table_path = get_delta_table_path(TARGET_CATALOG, TARGET_SCHEMA, ORDER_SUMMARY_TABLE)
        customer_aggregate_table_path = get_delta_table_path(TARGET_CATALOG, TARGET_SCHEMA, CUSTOMER_AGGREGATE_TABLE)
        
        logger.info(f"Reading order summary data from {order_summary_table_path}")
        
        # Read the ordersummary data
        order_summary_df = spark.read.format("delta").table(order_summary_table_path)
        
        # Check if required columns exist
        required_columns = ["Name", "TotalAmount", "OrderDate"]
        missing_columns = [col for col in required_columns if col not in order_summary_df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        logger.info("Aggregating customer spend data")
        
        # Extract date from OrderDate if it's a timestamp
        if "timestamp" in order_summary_df.schema["OrderDate"].dataType.simpleString():
            order_summary_df = order_summary_df.withColumn(
                "OrderDate", 
                date_format(col("OrderDate"), "yyyy-MM-dd")
            )
        
        # Aggregate TotalAmount by Name and Date
        aggregate_df = order_summary_df.groupBy("Name", "OrderDate") \
            .agg(spark_sum("TotalAmount").alias("TotalSpend"))
        
        # Write to customeraggregatespend table
        logger.info(f"Writing aggregated data to {customer_aggregate_table_path}")
        aggregate_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(customer_aggregate_table_path)
        
        logger.info("Successfully created customeraggregatespend table")
        return True
    
    except Exception as e:
        logger.error(f"Error creating customeraggregatespend table: {str(e)}")
        return False

def main():
    """Main function to create customeraggregatespend table."""
    spark = get_spark_session()
    
    success = create_customer_aggregate_spend(spark)
    
    if success:
        logger.info("Customer aggregate spend creation completed successfully")
    else:
        logger.error("Customer aggregate spend creation failed")

if __name__ == "__main__":
    main()