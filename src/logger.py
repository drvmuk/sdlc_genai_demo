"""
Logger module for the Finance Data Processor
"""
import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession

from src.config import LOG_PATH


def setup_logger(name: str) -> logging.Logger:
    """
    Set up and configure logger
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    # Add file handler for Databricks environment
    try:
        spark = SparkSession.builder.getOrCreate()
        log_file = f"{LOG_PATH}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Use Databricks dbutils to create log file handler
        dbutils = globals().get('dbutils')
        if dbutils:
            # Create log directory if it doesn't exist
            dbutils.fs.mkdirs(LOG_PATH)
            
            # Configure file handler
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"Failed to set up file logging: {str(e)}")
    
    return logger