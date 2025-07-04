"""
Initial data loader for SCD Type 2 implementation.
Loads customer and order data from CSV files and creates SCD Type 2 tables.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, col, when, coalesce
)
import datetime
from delta.tables import DeltaTable
import sys
import traceback

from src.config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH,
    CUSTOMER_TABLE_PATH, ORDER_TABLE_PATH, ORDER_SUMMARY_TABLE_PATH,
    EFFECTIVE_FROM_COL, EFFECTIVE_TO_COL, CURRENT_FLAG_COL, INFINITE_DATE
)
from src.logger import get_logger, log_job_start, log_job_end, log_step

# Initialize logger
logger = get_logger(__name__)

def create_spark_session():
    """
    Create and return a Spark session configured for Delta Lake.
    
    Returns:
        SparkSession: Configured Spark session
    """
    return (SparkSession.builder
            .appName("SCD Type 2 Data Loader")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def load_csv_to_delta(spark, source_path, target_table):
    """
    Load CSV data into a Delta table, removing null and duplicate records.
    
    Args:
        spark: SparkSession
        source_path: Path to source CSV files
        target_table: Full path to target Delta table
        
    Returns:
        DataFrame: The loaded and cleaned data
    """
    log_step(logger, f"Loading data from {source_path} to {target_table}")
    
    # Read CSV data with header and infer schema
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
    
    # Log the schema and count before cleaning
    logger.info(f"Source data schema: {df.schema}")
    logger.info(f"Source data count: {df.count()}")
    
    # Remove rows with null values in any column
    df_no_nulls = df.dropna()
    logger.info(f"Data count after removing nulls: {df_no_nulls.count()}")
    
    # Remove duplicate rows
    df_no_duplicates = df_no_nulls.dropDuplicates()
    logger.info(f"Data count after removing duplicates: {df_no_duplicates.count()}")
    
    # Write to Delta table
    df_no_duplicates.write.format("delta").mode("overwrite").saveAsTable(target_table)
    logger.info(f"Successfully loaded data to {target_table}")
    
    return df_no_duplicates

def create_scd_type2_table(spark, customer_df, order_df):
    """
    Create an SCD Type 2 table by joining customer and order data.
    
    Args:
        spark: SparkSession
        customer_df: Customer DataFrame
        order_df: Order DataFrame
        
    Returns:
        DataFrame: The joined data with SCD Type 2 columns
    """
    log_step(logger, "Creating SCD Type 2 table by joining customer and order data")
    
    # Join customer and order data
    joined_df = customer_df.join(order_df, "CustId", "inner")
    logger.info(f"Joined data count: {joined_df.count()}")
    
    # Add SCD Type 2 columns
    current_time = datetime.datetime.now()
    
    scd_df = (joined_df
              .withColumn(EFFECTIVE_FROM_COL, current_timestamp())
              .withColumn(EFFECTIVE_TO_COL, lit(INFINITE_DATE))
              .withColumn(CURRENT_FLAG_COL, lit(True)))
    
    # Write to Delta table
    scd_df.write.format("delta").mode("overwrite").saveAsTable(ORDER_SUMMARY_TABLE_PATH)
    logger.info(f"Successfully created SCD Type 2 table: {ORDER_SUMMARY_TABLE_PATH}")
    
    return scd_df

def main():
    """
    Main function to execute the SCD Type 2 data loading process.
    """
    log_job_start(logger, "SCD Type 2 Initial Data Load")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Load customer data
        customer_df = load_csv_to_delta(spark, CUSTOMER_DATA_PATH, CUSTOMER_TABLE_PATH)
        
        # Load order data
        order_df = load_csv_to_delta(spark, ORDER_DATA_PATH, ORDER_TABLE_PATH)
        
        # Create SCD Type 2 table
        create_scd_type2_table(spark, customer_df, order_df)
        
        log_job_end(logger, "SCD Type 2 Initial Data Load")
        
    except Exception as e:
        logger.error(f"Error in SCD Type 2 data loading process: {str(e)}")
        logger.error(traceback.format_exc())
        log_job_end(logger, "SCD Type 2 Initial Data Load", success=False)
        sys.exit(1)

if __name__ == "__main__":
    main()