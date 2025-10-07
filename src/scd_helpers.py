"""
Helper functions for implementing SCD Type 2 pattern.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr

def initialize_scd_table(df: DataFrame) -> DataFrame:
    """
    Initialize an SCD Type 2 table with first-time data.
    
    Args:
        df: Source DataFrame
        
    Returns:
        DataFrame with SCD Type 2 columns added
    """
    return (
        df
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None).cast("timestamp"))
        .withColumn("HashKey", expr("sha2(concat_ws('|', CustId, Name, EmailId, Region), 256)"))
    )

def identify_changed_records(current_df: DataFrame, existing_df: DataFrame) -> DataFrame:
    """
    Identify records that have changed between current and existing data.
    
    Args:
        current_df: Current DataFrame with HashKey
        existing_df: Existing DataFrame with HashKey
        
    Returns:
        DataFrame containing only the changed records
    """
    return (
        current_df.join(
            existing_df.filter(col("IsActive") == True).select("CustId", "HashKey"),
            on="CustId",
            how="inner"
        )
        .filter(current_df["HashKey"] != existing_df["HashKey"])
        .select(current_df["*"])
    )

def expire_old_records(existing_df: DataFrame, changed_cust_ids: DataFrame) -> DataFrame:
    """
    Expire old records that have changed.
    
    Args:
        existing_df: Existing DataFrame
        changed_cust_ids: DataFrame with CustId of changed records
        
    Returns:
        DataFrame with expired records
    """
    return (
        existing_df
        .filter(col("IsActive") == True)
        .join(changed_cust_ids.select("CustId"), on="CustId", how="inner")
        .withColumn("IsActive", lit(False))
        .withColumn("EndDate", current_timestamp())
    )

def create_new_active_records(changed_df: DataFrame) -> DataFrame:
    """
    Create new active records for changed data.
    
    Args:
        changed_df: DataFrame with changed records
        
    Returns:
        DataFrame with new active records
    """
    return (
        changed_df
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None).cast("timestamp"))
    )

def identify_new_records(current_df: DataFrame, existing_df: DataFrame) -> DataFrame:
    """
    Identify completely new records not present in existing data.
    
    Args:
        current_df: Current DataFrame
        existing_df: Existing DataFrame
        
    Returns:
        DataFrame with new records
    """
    return (
        current_df
        .join(
            existing_df.filter(col("IsActive") == True).select("CustId"),
            on="CustId",
            how="left_anti"
        )
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None).cast("timestamp"))
    )

def process_scd_type2_changes(current_df: DataFrame, existing_df: DataFrame) -> DataFrame:
    """
    Process SCD Type 2 changes between current and existing data.
    
    Args:
        current_df: Current DataFrame
        existing_df: Existing DataFrame
        
    Returns:
        DataFrame with SCD Type 2 changes applied
    """
    # Add hash key to current data
    current_data = (
        current_df
        .withColumn("HashKey", expr("sha2(concat_ws('|', CustId, Name, EmailId, Region), 256)"))
    )
    
    # Get existing active records
    existing_active = existing_df.filter(col("IsActive") == True)
    
    # Identify changed records
    changed_records = identify_changed_records(current_data, existing_active)
    
    # Expire old records
    records_to_expire = expire_old_records(existing_active, changed_records)
    
    # Create new active records
    new_active_records = create_new_active_records(changed_records)
    
    # Identify completely new records
    new_records = identify_new_records(current_data, existing_active)
    
    # Get unchanged records
    unchanged_records = (
        existing_active
        .join(changed_records.select("CustId"), on="CustId", how="left_anti")
    )
    
    # Combine all record types
    return (
        unchanged_records
        .unionByName(records_to_expire)
        .unionByName(new_active_records)
        .unionByName(new_records)
    )