"""
Main entry point for ETL pipeline
"""

from pyspark.sql import SparkSession
from etl_processor import ETLProcessor
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("main")

def get_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession
    
    Returns:
        Configured SparkSession
    """
    return SparkSession.builder \
        .appName("CustomerOrderETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def main():
    """
    Main function to execute ETL pipeline
    """
    try:
        logger.info("Starting ETL pipeline")
        
        # Get SparkSession
        spark = get_spark_session()
        
        # Initialize and run ETL processor
        etl_processor = ETLProcessor(spark)
        etl_processor.run_etl_pipeline()
        
        logger.info("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise
    finally:
        # Don't stop the SparkSession in Databricks environment
        # It's managed by the platform
        pass

if __name__ == "__main__":
    main()