"""
Main module to run the Delta tables processing
"""
from pyspark.sql import SparkSession
from delta_tables_loader import DeltaTablesLoader
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Main function to execute the Delta tables processing pipeline
    """
    try:
        # Create SparkSession
        spark = SparkSession.builder \
            .appName("DeltaTablesProcessing") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        logger.info("SparkSession created successfully")
        
        # Initialize DeltaTablesLoader
        loader = DeltaTablesLoader(spark)
        
        # Step 1: Load CSV data to Delta tables
        logger.info("Step 1: Loading CSV data to Delta tables")
        loader.load_csv_to_delta(
            "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata",
            "gen_ai_poc_databrickscoe.sdlc_wizard.customer_dlt"
        )
        loader.load_csv_to_delta(
            "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata",
            "gen_ai_poc_databrickscoe.sdlc_wizard.order_dlt"
        )
        
        # Step 2: Transform Delta tables
        logger.info("Step 2: Transforming Delta tables")
        loader.transform_delta_tables()
        
        # Step 3: Create order summary table
        logger.info("Step 3: Creating order summary table")
        loader.create_order_summary()
        
        # Step 4: Implement SCD Type 2 logic
        logger.info("Step 4: Implementing SCD Type 2 logic")
        loader.implement_scd_type2()
        
        # Step 5: Create customer aggregate spend table
        logger.info("Step 5: Creating customer aggregate spend table")
        loader.create_customer_aggregate_spend()
        
        logger.info("Delta tables processing completed successfully")
    except Exception as e:
        logger.error(f"Error in Delta tables processing: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()