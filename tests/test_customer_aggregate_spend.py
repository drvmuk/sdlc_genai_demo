"""
Unit tests for customer aggregate spend module.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType
import os
from datetime import datetime, date

# Import the module to test
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.customer_aggregate_spend import create_customer_aggregate_spend

class TestCustomerAggregateSpend(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestCustomerAggregateSpend") \
            .master("local[2]") \
            .getOrCreate()
        
        # Create test catalog and schema
        cls.catalog = "test_catalog"
        cls.schema = "test_schema"
        
        # Create test data
        cls.create_test_data()
    
    @classmethod
    def tearDownClass(cls):
        # Stop