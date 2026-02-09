"""
Workflow Runner for DPAddressChangeYUYU

This module provides the entry point for running the DPAddressChangeYUYU workflow.
It handles configuration, logging, and execution of the workflow.
"""
import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from dp_address_change_yuyu import run_workflow


def setup_logging():
    """Set up logging configuration."""
    log_dir = "/dbfs/mnt/logs/dp_address_change_yuyu"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"{log_dir}/dp_address_change_yuyu_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger("DPAddressChangeYUYU")


def get_config():
    """Get configuration from environment or default values."""
    return {
        "target_dir": os.environ.get("TARGET_FILE_DIR", "/dbfs/mnt/target/files"),
        "batch_size": int(os.environ.get("BATCH_SIZE", "10000"))
    }


def main():
    """Main entry point for the workflow."""
    logger = setup_logging()
    logger.info("Starting DPAddressChangeYUYU workflow")
    
    try:
        # Create SparkSession
        spark = SparkSession.builder \
            .appName("DPAddressChangeYUYU") \
            .enableHiveSupport() \
            .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
        
        # Get configuration
        config = get_config()
        logger.info(f"Using configuration: {config}")
        
        # Run workflow
        run_workflow(spark, config)
        
        logger.info("Workflow completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())