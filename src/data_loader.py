"""
Module for loading transaction data from various sources.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from typing import Optional, Dict, Any


def get_spark_session() -> SparkSession:
    """
    Get or create a Spark session.
    
    Returns:
        SparkSession: Active Spark session
    """
    return (SparkSession.builder
            .appName("TransactionAnalytics")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def load_transactions(spark: SparkSession, source_path: str, format_type: str = "delta") -> DataFrame:
    """
    Load transaction data from specified source.
    
    Args:
        spark: Active Spark session
        source_path: Path to the data source
        format_type: Format of the source data (delta, parquet, csv)
        
    Returns:
        DataFrame: Loaded transaction data
    """
    # Define schema for transactions
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("transaction_date", TimestampType(), False),
        StructField("amount", DoubleType(), False),
        StructField("category", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("is_online", StringType(), True)
    ])
    
    # Load data based on format
    if format_type == "csv":
        return spark.read.format("csv") \
            .option("header", "true") \
            .schema(transaction_schema) \
            .load(source_path)
    elif format_type in ["delta", "parquet"]:
        return spark.read.format(format_type) \
            .load(source_path)
    else:
        raise ValueError(f"Unsupported format type: {format_type}")


def load_customer_data(spark: SparkSession, source_path: str, format_type: str = "delta") -> DataFrame:
    """
    Load customer reference data.
    
    Args:
        spark: Active Spark session
        source_path: Path to the customer data
        format_type: Format of the source data
        
    Returns:
        DataFrame: Loaded customer data
    """
    # Define schema for customer data
    customer_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", TimestampType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("loyalty_tier", StringType(), True),
        StructField("age_group", StringType(), True)
    ])
    
    if format_type == "csv":
        return spark.read.format("csv") \
            .option("header", "true") \
            .schema(customer_schema) \
            .load(source_path)
    else:
        return spark.read.format(format_type) \
            .load(source_path)


def load_store_data(spark: SparkSession, source_path: str, format_type: str = "delta") -> DataFrame:
    """
    Load store reference data.
    
    Args:
        spark: Active Spark session
        source_path: Path to the store data
        format_type: Format of the source data
        
    Returns:
        DataFrame: Loaded store data
    """
    # Define schema for store data
    store_schema = StructType([
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("region", StringType(), True),
        StructField("store_type", StringType(), True)
    ])
    
    if format_type == "csv":
        return spark.read.format("csv") \
            .option("header", "true") \
            .schema(store_schema) \
            .load(source_path)
    else:
        return spark.read.format(format_type) \
            .load(source_path)