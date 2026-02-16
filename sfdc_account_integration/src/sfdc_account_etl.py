"""
SFDC Account Integration - ETL Module

This module implements the data integration pipeline for extracting Salesforce Account data
from SFDC_ABS.DIM_MPE_SF_ACCOUNT and loading it into STG_MPE_SF_ACCOUNT.
"""

import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, \
    DecimalType, DateType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Oracle connection configuration
ORACLE_SOURCE_CONFIG = {
    "url": "jdbc:oracle:thin:@UTDW014:1521:ORCL",
    "dbtable": "SFDC_ABS.DIM_MPE_SF_ACCOUNT",
    "user": "oracle_reader",
    "password": "{{secrets/oracle_reader}}",  # Reference to Databricks secret
    "driver": "oracle.jdbc.driver.OracleDriver",
    "fetchsize": "10000"  # Optimize for larger datasets
}

ORACLE_TARGET_CONFIG = {
    "url": "jdbc:oracle:thin:@UTDW014:1521:ORCL",
    "dbtable": "STG_MPE_SF_ACCOUNT",
    "user": "oracle_writer",
    "password": "{{secrets/oracle_writer}}",  # Reference to Databricks secret
    "driver": "oracle.jdbc.driver.OracleDriver",
    "batchsize": "10000"  # Optimize for larger datasets
}

def get_source_schema() -> StructType:
    """
    Define the schema for the source table DIM_MPE_SF_ACCOUNT.
    This is a partial schema with key fields - in production, all 125 columns would be defined.
    """
    return StructType([
        StructField("SK_ACCOUNT_ID", DecimalType(15, 0), False),
        StructField("DW_SF_ACCOUNT_ID", DecimalType(15, 0), False),
        StructField("DW_SF_USER_ID", DecimalType(15, 0), False),
        StructField("DW_SF_CUSTOMER_TYPE_ID", DecimalType(15, 0), False),
        StructField("SOURCEKEYID", StringType(), True),
        StructField("DELETED_FLG", StringType(), True),
        StructField("ACC_NAME", StringType(), True),
        StructField("ACC_PARTNER_REGION", StringType(), True),
        StructField("ACC_ACCOUNT_NUMBER", StringType(), True),
        StructField("ACC_PROSPECT_NUMBER", StringType(), True),
        StructField("ACC_BILLING_POSTAL_CODE", DecimalType(15, 0), True),
        StructField("ACC_SHIPPING_POSTAL_CODE", DecimalType(15, 0), True),
        StructField("LATITUDE", DecimalType(18, 15), True),
        StructField("LONGITUDE", DecimalType(18, 15), True),
        StructField("COMPANY_DESCRIPTION", StringType(), True),
        StructField("TAG_ACCOUNT_AS", StringType(), True),
        StructField("MPE_PROGRAM_STATUS", StringType(), True),
        StructField("PENDING_INACTIVATION_FLAG", StringType(), True),
        StructField("ACC_OPEN_OPP_DOLLARS_CURNCY_FY", DecimalType(18, 3), True),
        StructField("ACC_TOTAL_OPPORTUNITY_LOST", DecimalType(18, 3), True),
        StructField("ACC_TOTAL_OPPORTUNITY_OPEN", DecimalType(18, 3), True),
        StructField("ACC_TOTAL_OPPORTUNITY_WON", DecimalType(18, 3), True),
        StructField("SRC_ACC_CREATED_DATE", DateType(), True),
        StructField("SRC_ACC_LAST_MODIFIED_DATE", DateType(), True),
        StructField("SRC_ACCOUNT_CREATE_DATE", DateType(), True),
        StructField("DW_UPDATE_DT", DateType(), True),
        StructField("DW_CREATE_DT", DateType(), True),
        StructField("SUSPENDED_STATE", StringType(), True),
        # ... Additional fields would be defined here for all 125 columns
    ])

def extract_data(spark: SparkSession, update_dt: str) -> DataFrame:
    """
    Extract data from the source Oracle table with appropriate filters.
    
    Args:
        spark: SparkSession object
        update_dt: Update date parameter in MM/DD/YYYY format
        
    Returns:
        DataFrame containing filtered source data
    """
    logger.info(f"Extracting data with update date >= {update_dt}")
    
    # Convert update_dt to date format for filtering
    try:
        parsed_date = datetime.strptime(update_dt, "%m/%d/%Y")
    except ValueError:
        logger.error(f"Invalid date format: {update_dt}. Expected MM/DD/YYYY")
        raise
    
    # Format date for Oracle SQL filter
    oracle_date_format = parsed_date.strftime("%Y-%m-%d")
    
    # Build the query with filters
    query = f"""
    SELECT *
    FROM SFDC_ABS.DIM_MPE_SF_ACCOUNT
    WHERE ACC_PARTNER_REGION = 'NAMR'
    AND TRUNC(DW_UPDATE_DT) >= TO_DATE('{oracle_date_format}', 'YYYY-MM-DD')
    """
    
    # Read from Oracle source
    df = spark.read \
        .format("jdbc") \
        .options(**ORACLE_SOURCE_CONFIG) \
        .option("query", query) \
        .load()
    
    row_count = df.count()
    logger.info(f"Extracted {row_count} rows from source")
    
    return df

def transform_data(df: DataFrame) -> DataFrame:
    """
    Apply transformations to the source data.
    In this case, it's primarily field renaming with minimal logic.
    
    Args:
        df: Source DataFrame
        
    Returns:
        Transformed DataFrame
    """
    logger.info("Applying transformations")
    
    # Rename fields as per requirements
    transformed_df = df \
        .withColumnRenamed("ACC_ACCOUNT_NUMBER", "ACC_ACCOUNT_DECIMAL") \
        .withColumnRenamed("ACC_PROSPECT_NUMBER", "ACC_PROSPECT_DECIMAL")
    
    # All other fields are pass-through with no additional transformations
    
    return transformed_df

def load_data(df: DataFrame, target_config: Dict[str, Any]) -> None:
    """
    Load data to the target Oracle table.
    
    Args:
        df: Transformed DataFrame
        target_config: Oracle target configuration
    """
    logger.info("Loading data to target table")
    
    # Write to target table
    df.write \
        .format("jdbc") \
        .options(**target_config) \
        .mode("append") \
        .save()
    
    logger.info(f"Successfully loaded {df.count()} rows to target")

def extract_transform_load(spark: SparkSession, update_dt: str) -> None:
    """
    Main ETL function that orchestrates the extract, transform, and load steps.
    
    Args:
        spark: SparkSession object
        update_dt: Update date parameter in MM/DD/YYYY format
    """
    logger.info(f"Starting ETL process with update_dt={update_dt}")
    
    try:
        # Extract
        source_df = extract_data(spark, update_dt)
        
        # Transform
        transformed_df = transform_data(source_df)
        
        # Load
        load_data(transformed_df, ORACLE_TARGET_CONFIG)
        
        logger.info("ETL process completed successfully")
    
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='SFDC Account ETL Process')
    parser.add_argument('--update_dt', required=True, 
                        help='Update date in MM/DD/YYYY format')
    return parser.parse_args()

def create_spark_session() -> SparkSession:
    """Create and configure a Spark session."""
    return SparkSession.builder \
        .appName("SFDC Account Integration") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

if __name__ == "__main__":
    args = parse_arguments()
    spark = create_spark_session()
    
    extract_transform_load(spark, args.update_dt)