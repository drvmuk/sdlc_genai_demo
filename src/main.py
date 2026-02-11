"""
Main entry point for the E2E Policy Services data extraction pipeline.
"""
from pyspark.sql import SparkSession
import logging
from datetime import datetime

from .extraction import get_spark_session, extract_source_data
from .transformation import transform_policy_data
from .loading import load_to_target

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def run_pipeline():
    """
    Execute the full ETL pipeline.
    
    Returns:
        None
    """
    start_time = datetime.now()
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting E2E_BC_K2H_DATA_EXTRACTION pipeline")
    
    try:
        # Initialize Spark session
        spark = get_spark_session()
        logger.info("Spark session initialized")
        
        # Extract source data
        logger.info("Extracting source data")
        source_tables = extract_source_data(spark)
        logger.info("Source data extraction complete")
        
        # Transform data
        logger.info("Transforming data")
        transformed_data = transform_policy_data(source_tables)
        logger.info(f"Transformation complete. Generated {transformed_data.count()} records")
        
        # Load data to target
        logger.info("Loading data to target")
        load_to_target(spark, transformed_data)
        logger.info("Data loading complete")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Pipeline completed successfully in {duration} seconds")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    run_pipeline()