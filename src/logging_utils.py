"""
Utility functions for logging in Databricks.
"""
import logging
from datetime import datetime

# Configure logger
logger = logging.getLogger("etl_pipeline")
logger.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(console_handler)

def log_info(message):
    """Log info message"""
    logger.info(message)

def log_error(message, exception=None):
    """Log error message with optional exception"""
    if exception:
        logger.error(f"{message}: {str(exception)}")
    else:
        logger.error(message)

def log_start_job(job_name):
    """Log job start"""
    logger.info(f"Starting job: {job_name} at {datetime.now()}")

def log_end_job(job_name):
    """Log job end"""
    logger.info(f"Completed job: {job_name} at {datetime.now()}")