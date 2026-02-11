"""
Sample data generator for testing the E2E Policy Services data extraction pipeline.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from datetime import datetime, timedelta
import random

def create_sample_data(spark: SparkSession):
    """
    Create sample data for testing the ETL pipeline.
    
    Args:
        spark: SparkSession
        
    Returns:
        dict: Dictionary containing sample DataFrames
    """
    # Create schema for T_TX_REQUEST_POLICY
    request_policy_schema = StructType([
        StructField("REQUEST_POLICY_ID", IntegerType(), False),
        StructField("REQUEST_ID", IntegerType(), False),
        StructField("TRANSACTION_ID", IntegerType(), False),
        StructField("POLICY_ID", StringType(), False),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("BASE_CD", StringType(), True),
        StructField("SBASE_CD", StringType(), True),
        StructField("PSTA_CD", StringType(), True),
        StructField("PENTT_CD", StringType(), True),
        StructField("ORPOLDT", TimestampType(), True),
        StructField("PUSG_CD", StringType(), True),
        StructField("PUSG_EDT", TimestampType(), True),
        StructField("PUSG_PDT", TimestampType(), True),
        StructField("PWTV_CD", StringType(), True),
        StructField("PWTV2_CD", StringType(), True),
        StructField("CLASS_CODE