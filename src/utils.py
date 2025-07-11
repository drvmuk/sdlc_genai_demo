"""
Utility functions for the DLT pipeline.
"""
import logging
from datetime import datetime
from functools import wraps
import time

from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

from src.config import LOG_PATH, MAX_RETRIES, RETRY_DELAY

# Configure logging
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DLTPipeline")

def log_execution(func):
    """Decorator to log function execution details."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting execution of {func.__name__}")
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"Successfully completed {func.__name__} in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            raise
    return wrapper

def retry_operation(func):
    """Decorator to retry operations on failure."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        retries = 0
        while retries < MAX_RETRIES:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                retries += 1
                logger.warning(f"Attempt {retries} failed for {func.__name__}: {str(e)}")
                if retries >= MAX_RETRIES:
                    logger.error(f"All {MAX_RETRIES} attempts failed for {func.__name__}")
                    raise
                time.sleep(RETRY_DELAY)
    return wrapper

def add_audit_columns(df: DataFrame) -> DataFrame:
    """Add audit columns to a DataFrame."""
    return df.withColumn("created_at", current_timestamp()) \
             .withColumn("updated_at", current_timestamp())

def remove_nulls_and_duplicates(df: DataFrame, key_columns=None) -> DataFrame:
    """Remove null values and duplicate records from DataFrame."""
    # Remove rows with any null values
    df_no_nulls = df.dropna()
    
    # Remove duplicates based on key columns if specified, otherwise use all columns
    if key_columns:
        return df_no_nulls.dropDuplicates(key_columns)
    return df_no_nulls.dropDuplicates()