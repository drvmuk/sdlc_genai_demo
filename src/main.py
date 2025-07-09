"""
Main module to orchestrate the entire ETL process.
"""

from pyspark.sql import SparkSession
import logging
from data_loading import load_customer_data, load_order_data
from order_summary import create_order_summary, update_order_summary_on_customer_change
from customer_aggregate_spend import create_customer_aggregate_spend

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_etl_pipeline():
    """Run the complete ETL pipeline"""
    try:
        logger.info("Starting ETL pipeline")
        
        spark = SparkSession.builder \
            .appName("Delta Lake ETL Pipeline") \
            .getOrCreate()
        
        # Configuration
        catalog = "gen_ai_poc_databrickscoe"
        schema = "sdlc_wizard"
        customer_source_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
        order_source_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
        
        # Ensure schema exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        
        # Step 1: Load customer and order data
        logger.info("Step 1: Loading customer and order data")
        load_customer_data(spark, customer_source_path, catalog, schema)
        load_order_data(spark, order_source_path, catalog, schema)
        
        # Step 2: Create order summary
        logger.info("Step 2: Creating order summary")
        create_order_summary(spark, catalog, schema)
        
        # Step 3: Update order summary based on customer changes
        logger.info("Step 3: Updating order summary based on customer changes")
        update_order_summary_on_customer_change(spark, catalog, schema)
        
        # Step 4: Create customer aggregate spend
        logger.info("Step 4: Creating customer aggregate spend")
        create_customer_aggregate_spend(spark, catalog, schema)
        
        logger.info("ETL pipeline completed successfully")
    
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    run_etl_pipeline()