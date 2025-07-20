"""
Logging utility for the Order Processing System.
"""
import logging
from datetime import datetime

def get_logger(name):
    """
    Returns a configured logger instance.
    
    Args:
        name (str): Logger name, typically __name__ of the calling module
        
    Returns:
        logging.Logger: Configured logger instance
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format
    )
    
    logger = logging.getLogger(name)
    return logger

def log_job_start(logger, job_name):
    """Log job start with timestamp"""
    logger.info(f"Starting job: {job_name} at {datetime.now()}")

def log_job_end(logger, job_name):
    """Log job end with timestamp"""
    logger.info(f"Completed job: {job_name} at {datetime.now()}")