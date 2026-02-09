"""
Source record count check module for E2E Address Change YUYU Creation workflow.
Replaces the Informatica session s_E2E_AC_TXDBH_DP_YUYU_SRC_REC_CNT_CHK.
"""
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, lit

logger = logging.getLogger(__name__)

def check_source_records(spark: SparkSession, db_connection: str, target_file_dir: str) -> int:
    """
    Check if there are records to process in the source staging table.
    
    Args:
        spark: SparkSession object
        db_connection: Oracle staging database connection string
        target_file_dir: Directory for target files
    
    Returns:
        int: Count of source records
    """
    try:
        logger.info("Checking source records")
        
        # Read from source table
        source_df = spark.read \
            .format("jdbc") \
            .option("url", db_connection) \
            .option("dbtable", "ZSYSE2EDEV.STG_E2E_AC_TXDBH_DATA") \
            .option("user", os.environ.get("DB_USER")) \
            .option("password", os.environ.get("DB_PASSWORD")) \
            .load()
        
        # Filter records based on status or other criteria if needed
        # This would be the equivalent of the $$M_SRC_SQL parameter in Informatica
        
        # Count records
        count_df = source_df.agg(count("*").alias("SOURCE_REC_COUNT"))
        record_count = count_df.collect()[0]["SOURCE_REC_COUNT"]
        
        # Write count to file for audit/tracking
        output_file_path = f"{target_file_dir}/source_count.csv"
        
        count_df \
            .withColumn("SOURCE_RECT", count_df["SOURCE_REC_COUNT"]) \
            .select("SOURCE_RECT") \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_file_path)
        
        logger.info(f"Source record count: {record_count}")
        return record_count
        
    except Exception as e:
        logger.error(f"Error checking source records: {str(e)}", exc_info=True)
        raise