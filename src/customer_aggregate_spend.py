"""
Module for creating and loading the customer aggregate spend table.
Implements TR-DTLD-004.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_timestamp
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_customer_aggregate_spend(spark, catalog, schema):
    """
    Create and load customeraggregatespend table with aggregated data from ordersummary table.
    
    Args:
        spark: SparkSession
        catalog: Target catalog name
        schema: Target schema name
    """
    try:
        logger.info("Creating customer aggregate spend table")
        
        # Read source table
        order_summary_table = f"{catalog}.{schema}.ordersummary"
        target_table = f"{catalog}.{schema}.customeraggregatespend"
        
        # Only use active records for aggregation
        order_summary_df = spark.table(order_summary_table).filter(col("IsActive") == True)

        # order_summary_df = order_summary_df.withColumn("TotalAmount",lit(col("PricePerUnit") * col("Qty")))
        
        # Aggregate data
        aggregated_df = order_summary_df.groupBy("Name", "Date") \
            .agg(sum("TotalAmount").alias("TotalSpend")) \
            .withColumn("ProcessedTimestamp", current_timestamp())
        
        # Write to target table
        aggregated_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table)
        
        logger.info(f"Successfully created customer aggregate spend table: {target_table}")
        return True
    
    except Exception as e:
        logger.error(f"Error creating customer aggregate spend: {str(e)}")
        raise

if __name__ == "__main__":
    main()
