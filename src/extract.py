"""Data extraction module for the ETL pipeline."""

from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from .config import DataSource


def read_source(
    spark: SparkSession, 
    source: DataSource,
    filters: Optional[Dict[str, str]] = None
) -> DataFrame:
    """
    Read data from a source configuration.
    
    Args:
        spark: SparkSession instance
        source: DataSource configuration
        filters: Optional dictionary of filters to apply
        
    Returns:
        DataFrame with the source data
    """
    reader = spark.read.format(source.format)
    
    # Apply options if provided
    if source.options:
        for key, value in source.options.items():
            reader = reader.option(key, value)
    
    # Read the data
    df = reader.load(source.path)
    
    # Apply filters if provided
    if filters:
        for column, value in filters.items():
            df = df.filter(f"{column} = '{value}'")
    
    return df


def extract_data(
    spark: SparkSession,
    sources: List[DataSource],
    date: Optional[str] = None
) -> Dict[str, DataFrame]:
    """
    Extract data from all configured sources.
    
    Args:
        spark: SparkSession instance
        sources: List of DataSource configurations
        date: Optional date filter to apply
        
    Returns:
        Dictionary mapping source names to their DataFrames
    """
    dataframes = {}
    
    for source in sources:
        filters = {"date": date} if date and "date" in source.options.get("partitionBy", "") else None
        df = read_source(spark, source, filters)
        dataframes[source.name] = df
    
    return dataframes