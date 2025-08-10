"""
Tests for the data cleansing module
"""
import pytest
from pyspark.sql import SparkSession
import datetime

from src.data_cleansing import (
    remove_null_records,
    remove_duplicate_records
)

@pytest.fixture
def spark():
    """
    Create a Spark session for testing
    
    Returns:
        SparkSession: The Spark session
    """
    return SparkSession.builder \
        .appName("test-data-cleansing") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \