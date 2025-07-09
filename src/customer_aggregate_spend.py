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

def main():
    """Main function to execute customer aggregate spend creation"""
    try:
        spark = SparkSession.builder \
            .appName("Customer Aggregate Spend Processing") \
            .getOrCreate()
        
        # Configuration
        catalog = "gen_ai_poc_databrickscoe"
        schema = "sdlc_wizard"
        
        # Create customer aggregate spend
        create_customer_aggregate_spend(spark, catalog, schema)
        
        logger.info("Customer aggregate spend processing completed successfully")
    
    except Exception as e:
        logger.error(f"Error in customer aggregate spend processing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()