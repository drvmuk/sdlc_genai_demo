"""
Custom DQX rules for data quality validation.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when
import logging
from typing import Tuple, List, Any, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def make_condition(condition: DataFrame, message: str, function_name: str) -> Tuple[DataFrame, str, str]:
    """
    Helper function to create a DQX condition tuple.
    
    Args:
        condition: DataFrame with the condition result
        message: Error message to display
        function_name: Name of the function that created this condition
        
    Returns:
        Tuple containing (condition, message, function_name)
    """
    return (condition, message, function_name)

def dqx_null_check(df: DataFrame, columns: List[str], **kwargs) -> Tuple[DataFrame, str, str]:
    """
    Check if the specified columns contain null values.
    
    Args:
        df: Input DataFrame to check
        columns: List of column names to check for nulls
        
    Returns:
        DQX condition tuple
    """
    try:
        logger.info(f"Running null check on columns: {columns}")
        
        # Create condition for each column
        conditions = []
        for column in columns:
            if column not in df.columns:
                error_msg = f"Column '{column}' not found in DataFrame"
                logger.error(error_msg)
                raise ValueError(error_msg)
            conditions.append(col(column).isNull())
        
        # Combine conditions with OR
        if not conditions:
            error_msg = "No valid columns provided for null check"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition = combined_condition | condition
            
        # Create the condition DataFrame
        condition_df = df.select(~combined_condition)
        
        message = f"Columns {columns} should not contain NULL values"
        return make_condition(condition_df, message, "dqx_null_check")
        
    except Exception as e:
        logger.error(f"Error in dqx_null_check: {str(e)}")
        raise

def dqx_primary_check(df: DataFrame, columns: List[str], **kwargs) -> Tuple[DataFrame, str, str]:
    """
    Check if the specified columns form a unique key (primary key check).
    
    Args:
        df: Input DataFrame to check
        columns: List of column names that should form a unique key
        
    Returns:
        DQX condition tuple
    """
    try:
        logger.info(f"Running primary key check on columns: {columns}")
        
        # Validate columns exist in DataFrame
        for column in columns:
            if column not in df.columns:
                error_msg = f"Column '{column}' not found in DataFrame"
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        if not columns:
            error_msg = "No columns provided for primary key check"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        # Count occurrences of each combination of values in the specified columns
        duplicate_counts = df.groupBy(*columns).count().filter(col("count") > 1)
        
        # Check if there are any duplicates
        has_no_duplicates = duplicate_counts.count() == 0
        
        # Create condition DataFrame
        condition_df = df.select(df[0].lit(has_no_duplicates))
        
        message = f"Columns {columns} should form a unique key"
        return make_condition(condition_df, message, "dqx_primary_check")
        
    except Exception as e:
        logger.error(f"Error in dqx_primary_check: {str(e)}")
        raise