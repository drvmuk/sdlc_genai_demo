"""
Utility functions for customer order processing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when
from typing import List, Dict, Any

def get_spark_session() -> SparkSession:
    """
    Get or create a Spark session.
    
    Returns:
        SparkSession: The active Spark session
    """
    return SparkSession.builder.getOrCreate()

def validate_data(df, required_columns: List[str]) -> bool:
    """
    Validate if DataFrame contains required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present
        
    Returns:
        bool: True if all required columns are present
    """
    df_columns = df.columns
    missing_columns = [col for col in required_columns if col not in df_columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    
    return True

def apply_scd_type2_changes(
    target_df, 
    source_df, 
    join_columns: List[str], 
    compare_columns: List[str]
) -> Dict[str, Any]:
    """
    Apply SCD Type 2 changes to a target DataFrame.
    
    Args:
        target_df: Target DataFrame with existing data
        source_df: Source DataFrame with new/changed data
        join_columns: List of columns to join on
        compare_columns: List of columns to compare for changes
        
    Returns:
        Dict with DataFrames for records to insert and expire
    """
    spark = get_spark_session()
    
    # Only consider active records in the target
    active_records = target_df.filter(col("IsActive") == True)
    
    # Join source and target to find changes
    joined = active_records.join(
        source_df,
        join_columns,
        "full_outer"
    )
    
    # Create change detection expression
    change_condition = None
    for column in compare_columns:
        column_condition = (
            (active_records[column].isNull() & source_df[column].isNotNull()) |
            (active_records[column].isNotNull() & source_df[column].isNull()) |
            (active_records[column] != source_df[column])
        )
        
        if change_condition is None:
            change_condition = column_condition
        else:
            change_condition = change_condition | column_condition
    
    # Find records to expire (changed records that exist in target)
    records_to_expire = (
        joined
        .filter(change_condition)
        .filter(active_records[join_columns[0]].isNotNull())
        .select(active_records["*"])
        .withColumn("IsActive", lit(False))
        .withColumn("EndDate", current_timestamp())
    )
    
    # Find records to insert (new or changed records)
    records_to_insert = (
        joined
        .filter(
            change_condition | 
            active_records[join_columns[0]].isNull()
        )
        .filter(source_df[join_columns[0]].isNotNull())
        .select(source_df["*"])
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None).cast("timestamp"))
    )
    
    return {
        "to_expire": records_to_expire,
        "to_insert": records_to_insert
    }