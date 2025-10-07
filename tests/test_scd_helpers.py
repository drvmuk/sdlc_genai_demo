"""
Unit tests for SCD Type 2 helper functions.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
from src.scd_helpers import (
    initialize_scd_table, identify_changed_records, expire_old_records,
    create_new_active_records, identify_new_records, process_scd_type2_changes
)
from pyspark.sql.functions import col, lit

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestSCDHelpers") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture(scope="module")
def sample_joined_data(spark):
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("TotalAmount", DoubleType(), True)
    ])
    
    data = [
        ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1000.0, 1, 
         datetime.date(2023, 1, 15), 1000.0),
        ("C002", "Jane Smith", "jane@example.com", "South", "O002", "Mouse", 25.0, 2, 
         datetime.date(2023, 1, 16), 50.0),
        ("C003", "Bob Johnson", "bob@example.com", "East", "O003", "Keyboard", 50.0, 1, 
         datetime.date(2023, 1, 17), 50.0)
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def sample_existing_data(spark):
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("TotalAmount", DoubleType(), True),
        StructField("IsActive", StringType(), True),
        StructField("StartDate", DateType(), True),
        StructField("EndDate", DateType(), True),
        StructField("HashKey", StringType(), True)
    ])
    
    data = [
        ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1000.0, 1, 
         datetime.date(2023, 1, 15), 1000.0, "true", datetime.date(2023, 1, 1), None, "hash1"),
        ("C002", "Jane Smith", "jane@example.com", "West", "O002", "Mouse", 25.0, 2, 
         datetime.date(2023, 1, 16), 50.0, "true", datetime.date(2023, 1, 1), None, "hash2"),
        ("C004", "Alice Brown", "alice@example.com", "East", "O004", "Monitor", 200.0, 1, 
         datetime.date(2023, 1, 18), 200.0, "true", datetime.date(2023, 1, 1), None, "hash4")
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def sample_current_data(spark, sample_joined_data):
    # Add HashKey to sample joined data to simulate current data
    return sample_joined_data.withColumn(
        "HashKey", 
        lit("newhash")