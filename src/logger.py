"""
Logging utility for the customer order pipeline
"""
import logging
from datetime import datetime

# Configure logging
def get_logger(name):
    """
    Returns a configured logger instance
    
    Args:
        name: Name for the logger
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
    
    return logger

def log_job_metrics(logger, job_name, start_time, end_time=None, records_processed=None, status="Running"):
    """
    Log job execution metrics
    
    Args:
        logger: Logger instance
        job_name: Name of the job
        start_time: Job start time
        end_time: Job end time (optional)
        records_processed: Number of records processed (optional)
        status: Job status (default: "Running")
    """
    if end_time is None:
        end_time = datetime.now()
        
    duration = (end_time - start_time).total_seconds()
    
    metrics = {
        "job_name": job_name,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration,
        "status": status
    }
    
    if records_processed is not None:
        metrics["records_processed"] = records_processed
        
    logger.info(f"Job Metrics: {metrics}")