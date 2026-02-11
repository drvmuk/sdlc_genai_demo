"""
Data extraction module for E2E Policy Services.
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List
from .config import DB_CONFIG, SOURCE_TABLES

def get_spark_session() -> SparkSession:
    """
    Create and return a SparkSession.
    
    Returns:
        SparkSession: The configured SparkSession
    """
    return (SparkSession.builder
            .appName("E2E_BC_K2H_DATA_EXTRACTION")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())

def read_source_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Read a source table from the database.
    
    Args:
        spark: SparkSession
        table_name: Name of the table to read
        
    Returns:
        DataFrame: The table data as a DataFrame
    """
    source_config = DB_CONFIG["source"]
    
    return (spark.read
            .format("jdbc")
            .option("url", source_config["jdbc_url"])
            .option("dbtable", SOURCE_TABLES[table_name])
            .option("user", source_config["user"])
            .option("password", source_config["password"])
            .option("driver", source_config["driver"])
            .load())

def extract_source_data(spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Extract all required source tables.
    
    Args:
        spark: SparkSession
        
    Returns:
        Dict[str, DataFrame]: Dictionary of table names and their DataFrames
    """
    source_tables = {}
    
    for table_name in SOURCE_TABLES.keys():
        print(f"Extracting {table_name}...")
        source_tables[table_name] = read_source_table(spark, table_name)
        
    return source_tables