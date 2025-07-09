"""
Module for loading customer and order data into Delta tables.
Implements TR-DTLD-001.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_customer_data(spark, source_path, catalog, schema):
    """
    Load customer data from CSV to Delta table
    
    Args:
        spark: SparkSession
        source_path: Path to customer CSV data
        catalog: Target catalog name
        schema: Target schema name
    """
    try:
        logger.info(f"Loading customer data from {source_path}")
        
        # Read CSV data
        customer_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        # Clean data - remove nulls and duplicates
        customer_df = customer_df \
            .na.drop() \
            .dropDuplicates()
        
        # Add metadata columns
        customer_df = customer_df \
            .withColumn("LoadTimestamp", current_timestamp())
        
        # Write to Delta table
        table_name = f"{catalog}.{schema}.customer"
        customer_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)
        
        logger.info(f"Successfully loaded customer data to {table_name}")
        return True
    
    except Exception as e:
        logger.error(f"Error loading customer data: {str(e)}")
        raise

def load_order_data(spark, source_path, catalog, schema):
    """
    Load order data from CSV to Delta table
    
    Args:
        spark: SparkSession
        source_path: Path to order CSV data
        catalog: Target catalog name
        schema: Target schema name
    """
    try:
        logger.info(f"Loading order data from {source_path}")
        
        # Read CSV data
        order_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        # Clean data - remove nulls and duplicates
        order_df = order_df \
            .na.drop() \
            .dropDuplicates()
        
        # Add metadata columns
        order_df = order_df \
            .withColumn("LoadTimestamp", current_timestamp())
        
        # Write to Delta table
        table_name = f"{catalog}.{schema}.order"
        order_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)
        
        logger.info(f"Successfully loaded order data to {table_name}")
        return True
    
    except Exception as e:
        logger.error(f"Error loading order data: {str(e)}")
        raise

def main():
    """Main function to execute data loading process"""
    try:
        spark = SparkSession.builder \
            .appName("Customer and Order Data Loading") \
            .getOrCreate()
        
        # Configuration
        catalog = "gen_ai_poc_databrickscoe"
        schema = "sdlc_wizard"
        customer_source_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
        order_source_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
        
        # Ensure schema exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        
        # Load data
        load_customer_data(spark, customer_source_path, catalog, schema)
        load_order_data(spark, order_source_path, catalog, schema)
        
        logger.info("Data loading process completed successfully")
    
    except Exception as e:
        logger.error(f"Error in data loading process: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()