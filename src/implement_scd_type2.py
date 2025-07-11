"""
Module to implement SCD Type 2 for ordersummary table.
Implementation of TR-DLT-005.
"""
from pyspark.sql.functions import col, current_timestamp, lit, when
from delta.tables import DeltaTable
from utils import get_spark_session, setup_logger, get_delta_table_path
from config import TARGET_CATALOG, TARGET_SCHEMA, ORDER_SUMMARY_TABLE, SCD_TRACKING_COLUMNS

logger = setup_logger("implement_scd_type2")

def implement_scd_type2(spark, new_data=None):
    """
    Implement SCD Type 2 for the ordersummary table.
    
    Args:
        spark: SparkSession
        new_data: DataFrame with new/updated data (optional, for testing)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the ordersummary table path
        order_summary_table_path = get_delta_table_path(TARGET_CATALOG, TARGET_SCHEMA, ORDER_SUMMARY_TABLE)
        
        logger.info(f"Implementing SCD Type 2 for {order_summary_table_path}")
        
        # Read the current ordersummary data
        current_df = spark.read.format("delta").table(order_summary_table_path)
        
        # If no new data is provided (normal operation), simulate changes for testing
        if new_data is None:
            # In a real scenario, new_data would come from updated source systems
            # For this example, we'll simulate changes by modifying a sample of the current data
            logger.info("Using current data to simulate changes for SCD Type 2 implementation")
            
            # Get a sample of the data to modify (e.g., 10% of records)
            sample_size = int(current_df.count() * 0.1) or 1  # Ensure at least 1 record
            
            # Create sample with modified data
            sample_df = current_df.limit(sample_size)
            
            # Apply some changes to the tracking columns (e.g., update address)
            if "Address" in current_df.columns:
                new_data = sample_df.withColumn(
                    "Address", 
                    when(col("Address").isNotNull(), col("Address") + " (Updated)")
                    .otherwise(col("Address"))
                )
            else:
                # If Address doesn't exist, modify another column
                modify_col = SCD_TRACKING_COLUMNS[0] if SCD_TRACKING_COLUMNS else current_df.columns[1]
                new_data = sample_df.withColumn(
                    modify_col, 
                    when(col(modify_col).isNotNull(), col(modify_col) + " (Updated)")
                    .otherwise(col(modify_col))
                )
        
        # Get the primary key column (assuming first column is the primary key)
        primary_key = current_df.columns[0]
        
        # Convert to DeltaTable for merge operation
        delta_table = DeltaTable.forPath(spark, order_summary_table_path)
        
        # Create condition for matching records
        match_condition = f"target.{primary_key} = source.{primary_key} AND target.IsActive = true"
        
        # Create condition to detect changes in tracking columns
        change_condition = " OR ".join([f"target.{col} <> source.{col}" for col in SCD_TRACKING_COLUMNS])
        
        logger.info(f"Applying SCD Type 2 updates with match condition: {match_condition}")
        logger.info(f"Change detection on columns: {SCD_TRACKING_COLUMNS}")
        
        # Perform the merge operation
        delta_table.alias("target").merge(
            new_data.alias("source"),
            match_condition
        ).whenMatchedUpdate(
            condition=change_condition,
            set={
                "IsActive": "false",
                "EndDate": current_timestamp()
            }
        ).execute()
        
        # Insert new records for the changed rows
        changed_records = spark.read.format("delta").table(order_summary_table_path) \
            .filter(col("EndDate").isNotNull() & (col("EndDate") > current_timestamp() - lit(60))) \
            .join(new_data, primary_key, "inner")
        
        if changed_records.count() > 0:
            logger.info(f"Inserting {changed_records.count()} new active records")
            
            # Prepare new records with updated SCD Type 2 fields
            new_active_records = new_data \
                .withColumn("IsActive", lit(True)) \
                .withColumn("StartDate", current_timestamp()) \
                .withColumn("EndDate", lit(None).cast("timestamp"))
            
            # Append new active records
            new_active_records.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(order_summary_table_path)
        
        logger.info("Successfully implemented SCD Type 2 for ordersummary table")
        return True
    
    except Exception as e:
        logger.error(f"Error implementing SCD Type 2: {str(e)}")
        return False

def main():
    """Main function to implement SCD Type 2 for ordersummary table."""
    spark = get_spark_session()
    
    success = implement_scd_type2(spark)
    
    if success:
        logger.info("SCD Type 2 implementation completed successfully")
    else:
        logger.error("SCD Type 2 implementation failed")

if __name__ == "__main__":
    main()