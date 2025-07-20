"""
Utility functions for the data processing pipeline.
"""
import logging
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when

def setup_logger(name):
    """
    Set up a logger with the given name.
    
    Args:
        name (str): Logger name
        
    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create console handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    
    return logger

def validate_dataframe(df, mandatory_columns, logger):
    """
    Validate that a DataFrame contains the mandatory columns.
    
    Args:
        df (DataFrame): DataFrame to validate
        mandatory_columns (list): List of mandatory column names
        logger (logging.Logger): Logger instance
        
    Returns:
        bool: True if validation passes, False otherwise
    """
    missing_columns = [col for col in mandatory_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing mandatory columns: {missing_columns}")
        return False
    return True

def remove_null_records(df, mandatory_columns, logger):
    """
    Remove records with null values in mandatory columns.
    
    Args:
        df (DataFrame): DataFrame to process
        mandatory_columns (list): List of mandatory column names
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: DataFrame with null records removed
    """
    initial_count = df.count()
    
    for column in mandatory_columns:
        df = df.filter(col(column).isNotNull())
    
    final_count = df.count()
    removed_count = initial_count - final_count
    
    if removed_count > 0:
        logger.info(f"Removed {removed_count} records with null values in mandatory columns")
    
    return df

def remove_duplicate_records(df, key_columns, logger):
    """
    Remove duplicate records based on key columns.
    
    Args:
        df (DataFrame): DataFrame to process
        key_columns (list): List of key column names
        logger (logging.Logger): Logger instance
        
    Returns:
        DataFrame: DataFrame with duplicate records removed
    """
    initial_count = df.count()
    df = df.dropDuplicates(key_columns)
    final_count = df.count()
    removed_count = initial_count - final_count
    
    if removed_count > 0:
        logger.info(f"Removed {removed_count} duplicate records based on {key_columns}")
    
    return df

def apply_scd_type2(current_df, new_df, business_keys, compare_cols, 
                    effective_from_col="EffectiveFrom", 
                    effective_to_col="EffectiveTo", 
                    is_current_col="IsCurrent"):
    """
    Apply SCD Type 2 logic to track changes in data.
    
    Args:
        current_df (DataFrame): Current DataFrame
        new_df (DataFrame): New DataFrame with changes
        business_keys (list): List of business key columns
        compare_cols (list): List of columns to compare for changes
        effective_from_col (str): Column name for effective from date
        effective_to_col (str): Column name for effective to date
        is_current_col (str): Column name for is current flag
        
    Returns:
        tuple: (DataFrame of unchanged records, DataFrame of updated records, DataFrame of new records)
    """
    # Add SCD Type 2 columns to new data if they don't exist
    if effective_from_col not in new_df.columns:
        new_df = new_df.withColumn(effective_from_col, current_timestamp())
    if effective_to_col not in new_df.columns:
        new_df = new_df.withColumn(effective_to_col, lit(None))
    if is_current_col not in new_df.columns:
        new_df = new_df.withColumn(is_current_col, lit(True))
    
    # Identify new records
    joined_df = new_df.join(current_df, business_keys, "left_anti")
    new_records = joined_df.select(new_df.columns)
    
    # Identify changed records
    join_condition = []
    for key in business_keys:
        join_condition.append(new_df[key] == current_df[key])
    
    # Add conditions to check if any of the compared columns have changed
    change_condition = None
    for col_name in compare_cols:
        if change_condition is None:
            change_condition = (new_df[col_name] != current_df[col_name])
        else:
            change_condition = change_condition | (new_df[col_name] != current_df[col_name])
    
    # Join with current data to find changes
    if change_condition is not None:
        joined_df = new_df.join(current_df, join_condition, "inner").filter(change_condition)
        
        # Create updated records (old records with end date)
        updated_old_records = current_df.join(
            joined_df.select([current_df[key] for key in business_keys]).distinct(),
            business_keys,
            "inner"
        ).withColumn(effective_to_col, current_timestamp()) \
         .withColumn(is_current_col, lit(False))
        
        # Create new version of changed records
        updated_new_records = new_df.join(
            joined_df.select([new_df[key] for key in business_keys]).distinct(),
            business_keys,
            "inner"
        )
        
        # Get unchanged records
        unchanged_records = current_df.join(
            joined_df.select([current_df[key] for key in business_keys]).distinct(),
            business_keys,
            "left_anti"
        )
        
        return unchanged_records, updated_old_records, updated_new_records, new_records
    else:
        # If no changes, all records are either unchanged or new
        return current_df, None, None, new_records