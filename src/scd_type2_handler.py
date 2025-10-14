from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from typing import List, Optional

class SCDType2Handler:
    """
    Utility class to handle SCD Type 2 operations on Delta tables
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the SCD Type 2 handler
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def merge_scd_type2(
        self,
        target_table_path: str,
        source_df: DataFrame,
        join_columns: List[str],
        track_columns: List[str],
        sequence_by: str = "StartDate",
        effective_date_col: str = "StartDate",
        end_date_col: str = "EndDate",
        is_active_col: str = "IsActive"
    ) -> None:
        """
        Merge source data into a target Delta table using SCD Type 2 pattern
        
        Args:
            target_table_path: Path to the target Delta table
            source_df: Source DataFrame with new/updated data
            join_columns: List of columns to join source and target
            track_columns: List of columns to track changes for SCD Type 2
            sequence_by: Column to sequence versions by (usually a timestamp)
            effective_date_col: Column name for the effective start date
            end_date_col: Column name for the effective end date
            is_active_col: Column name for the active record indicator
        """
        current_timestamp = F.current_timestamp()
        
        # Check if target table exists
        try:
            target_table = DeltaTable.forPath(self.spark, target_table_path)
            target_exists = True
        except:
            target_exists = False
        
        # If target doesn't exist, create it with the source data
        if not target_exists:
            source_df = source_df.withColumn(effective_date_col, current_timestamp) \
                                .withColumn(end_date_col, F.lit(None)) \
                                .withColumn(is_active_col, F.lit(True))
            source_df.write.format("delta").mode("overwrite").save(target_table_path)
            return
        
        # Define join condition for the merge
        join_condition = " AND ".join([f"target.{col} = source.{col}" for col in join_columns])
        
        # Define change detection condition
        change_condition = " OR ".join([f"target.{col} <> source.{col}" for col in track_columns])
        
        # Execute merge operation
        target_table.alias("target") \
            .merge(
                source_df.alias("source"),
                f"target.{is_active_col} = true AND ({join_condition})"
            ) \
            .whenMatchedUpdate(
                condition=change_condition,
                set={
                    is_active_col: "false",
                    end_date_col: "current_timestamp()"
                }
            ) \
            .execute()
        
        # Insert new records for changed data
        target_df = target_table.toDF()
        
        # Get records that were just expired
        expired_records = target_df.filter(
            (F.col(is_active_col) == False) & 
            (F.col(end_date_col) == current_timestamp)
        )
        
        if expired_records.count() > 0:
            # Join with source to get updated values
            new_records = expired_records.alias("expired").join(
                source_df.alias("source"),
                join_columns,
                "inner"
            ).select(
                *[F.col(f"source.{col}").alias(col) for col in source_df.columns],
                F.lit(True).alias(is_active_col),
                current_timestamp.alias(effective_date_col),
                F.lit(None).cast("timestamp").alias(end_date_col)
            )
            
            # Insert the new records
            new_records.write.format("delta").mode("append").save(target_table_path)
        
        # Insert completely new records
        existing_keys = target_df.select(*join_columns).distinct()
        new_data = source_df.join(
            existing_keys,
            join_columns,
            "left_anti"
        ).withColumn(effective_date_col, current_timestamp) \
          .withColumn(end_date_col, F.lit(None)) \
          .withColumn(is_active_col, F.lit(True))
        
        if new_data.count() > 0:
            new_data.write.format("delta").mode("append").save(target_table_path)