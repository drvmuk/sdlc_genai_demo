"""
Source Record Count Check for E2E Policy Services

This module implements the source record count check for reconciliation
and audit purposes as defined in the S2T document for mapping
m_E2E_AC_TXDBH_DP_YUYU_SRC_REC_CNT_CHK.
"""

import os
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """
    Create and configure a Spark session
    
    Returns:
        SparkSession: Configured Spark session
    """
    return (SparkSession.builder
            .appName("E2E_AC_TXDBH_DP_YUYU_SRC_REC_CNT_CHK")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())

def extract_source_data(spark: SparkSession, m_src_sql: str) -> DataFrame:
    """
    Extract data from the source system using the provided SQL
    
    Args:
        spark: Active Spark session
        m_src_sql: SQL query to extract source data
    
    Returns:
        DataFrame: Extracted source data
    """
    logger.info(f"Extracting source data using provided SQL")
    
    try:
        # Execute the SQL query against the Oracle source
        # Note: In a real implementation, you would use proper JDBC connection parameters
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:oracle:thin:@//oracle-host:1521/service_name") \
            .option("dbtable", f"({m_src_sql})") \
            .option("user", "ZSYSE2EDEV") \
            .option("password", "{{secrets/oracle/password}}") \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
            
        logger.info(f"Successfully extracted source data")
        return df
    
    except Exception as e:
        logger.error(f"Error extracting source data: {str(e)}")
        raise

def calculate_source_count(df: DataFrame) -> DataFrame:
    """
    Calculate the source record count
    
    Args:
        df: Source DataFrame
    
    Returns:
        DataFrame: DataFrame with a single row containing the count
    """
    logger.info("Calculating source record count")
    
    # Using COUNT(*) to get total record count as per BR-3
    # This ensures all rows are counted regardless of NULL values
    count_df = df.agg(F.count("*").alias("SOURCE_REC_COUNT"))
    
    # Convert to string with max length 10 as per target field definition
    count_df = count_df.withColumn("SOURCE_RECT", 
                                   F.when(F.length(F.col("SOURCE_REC_COUNT").cast("string")) <= 10,
                                          F.col("SOURCE_REC_COUNT").cast("string"))
                                   .otherwise(F.lit("OVERFLOW")))
    
    # Check for overflow condition
    if count_df.filter(F.col("SOURCE_RECT") == "OVERFLOW").count() > 0:
        logger.warning("Count value exceeds maximum length of 10 characters")
    
    return count_df.select("SOURCE_RECT")

def write_to_target(df: DataFrame, output_path: str, output_filename: str) -> None:
    """
    Write the count result to the target flat file
    
    Args:
        df: DataFrame with the count result
        output_path: Directory path for output
        output_filename: Name of the output file
    """
    logger.info(f"Writing source count to target file: {output_path}/{output_filename}")
    
    # Ensure output directory exists
    full_path = os.path.join(output_path, output_filename)
    
    try:
        # Write as CSV with no header (as per flat file requirements)
        df.coalesce(1).write.mode("overwrite").format("csv") \
            .option("header", "false") \
            .option("delimiter", ",") \
            .save(f"{output_path}/temp")
        
        # Rename the part file to the desired filename
        # In a real implementation, you might use dbutils.fs.mv in Databricks
        spark = SparkSession.getActiveSession()
        temp_file = spark.read.format("text").load(f"{output_path}/temp").coalesce(1)
        temp_file.write.mode("overwrite").format("text").save(full_path)
        
        # Clean up temp directory
        # In Databricks: dbutils.fs.rm(f"{output_path}/temp", True)
        
        logger.info(f"Successfully wrote source count to {full_path}")
    
    except Exception as e:
        logger.error(f"Error writing to target: {str(e)}")
        raise

def run_source_count_check(
    m_src_sql: str,
    output_path: str,
    output_filename: Optional[str] = None
) -> None:
    """
    Main function to run the source count check process
    
    Args:
        m_src_sql: SQL query to extract source data
        output_path: Directory path for output
        output_filename: Name of the output file (optional)
    """
    start_time = datetime.now()
    logger.info(f"Starting source count check process at {start_time}")
    
    if not output_filename:
        # Generate default filename with timestamp if not provided
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"SOURCE_COUNT_{timestamp}.csv"
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Extract source data
        source_df = extract_source_data(spark, m_src_sql)
        
        # Calculate source count
        count_df = calculate_source_count(source_df)
        
        # Write to target
        write_to_target(count_df, output_path, output_filename)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Source count check completed successfully in {duration} seconds")
        
    except Exception as e:
        logger.error(f"Source count check failed: {str(e)}")
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run source record count check")
    parser.add_argument("--m-src-sql", required=True, help="SQL query for source data extraction")
    parser.add_argument("--output-path", required=True, help="Output directory path")
    parser.add_argument("--output-filename", help="Output filename")
    
    args = parser.parse_args()
    
    run_source_count_check(
        m_src_sql=args.m_src_sql,
        output_path=args.output_path,
        output_filename=args.output_filename
    )