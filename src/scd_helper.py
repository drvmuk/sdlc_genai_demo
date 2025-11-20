from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when

def apply_scd_type2_changes(current_df: DataFrame, new_df: DataFrame, key_columns: list, change_columns: list) -> DataFrame:
    """
    Apply SCD Type 2 changes to a Delta table.
    
    Args:
        current_df: Current data in the SCD Type 2 table
        new_df: New data to be merged
        key_columns: List of column names that form the business key
        change_columns: List of column names to check for changes
        
    Returns:
        DataFrame with SCD Type 2 changes applied
    """
    # Prepare new data with SCD Type 2 columns
    new_df_with_scd = new_df.withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", current_timestamp()) \
        .withColumn("EndDate", lit(None).cast("timestamp"))
    
    # Filter only active records from current data
    current_active_df = current_df.filter(col("IsActive") == True)
    
    # Identify records that exist in both datasets (for potential updates)
    join_condition = " AND ".join([f"current.{key} = new.{key}" for key in key_columns])
    
    # Create a view of current active records
    current_active_df.createOrReplaceTempView("current_active")
    
    # Create a view of new records
    new_df_with_scd.createOrReplaceTempView("new_data")
    
    # Find changed records
    change_conditions = " OR ".join([f"current.{col_name} <> new.{col_name}" for col_name in change_columns])
    
    # SQL to identify changed records
    changed_records_sql = f"""
    SELECT current.*
    FROM current_active current
    JOIN new_data new
    ON {join_condition}
    WHERE {change_conditions}
    """
    
    # Get records that have changed
    spark = current_df.sparkSession
    changed_records = spark.sql(changed_records_sql)
    
    # Mark changed records as inactive
    expired_records = changed_records.withColumn("IsActive", lit(False)) \
        .withColumn("EndDate", current_timestamp())
    
    # Get records from current that don't have changes
    unchanged_records = current_df.join(
        changed_records.select(*key_columns),
        on=key_columns,
        how="left_anti"
    )
    
    # Get new records that don't exist in current data
    new_records = new_df_with_scd.join(
        current_active_df.select(*key_columns),
        on=key_columns,
        how="left_anti"
    )
    
    # Union all parts to create the final dataset
    final_df = unchanged_records.unionByName(expired_records).unionByName(new_records)
    
    return final_df