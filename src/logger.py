"""
Logger utility for the SCD Type 2 data pipeline.
"""
import logging
import datetime

def get_logger(name):
    """
    Create and configure a logger for the specified module.
    
    Args:
        name: The name of the module requesting the logger
        
    Returns:
        A configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure the logger if it hasn't been configured already
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create console handler
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
    
    return logger

def log_job_start(logger, job_name):
    """Log job start with timestamp"""
    logger.info(f"Job '{job_name}' started at {datetime.datetime.now()}")

def log_job_end(logger, job_name, success=True):
    """Log job end with timestamp and status"""
    status = "successfully" if success else "with errors"
    logger.info(f"Job '{job_name}' completed {status} at {datetime.datetime.now()}")

def log_step(logger, step_name):
    """Log the start of a processing step"""
    logger.info(f"Starting step: {step_name}")