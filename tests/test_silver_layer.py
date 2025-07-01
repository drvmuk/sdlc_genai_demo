import unittest
from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
import datetime

# Import the module to test
sys.path.append("../src")
import silver_layer

class TestSilverLayer(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestSilverLayer") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create sample customer data
        customer_data = [
            ("1", "John Doe", "john@example.com", 30, datetime.datetime.now(), datetime.datetime.now(), True),
            ("2", "Jane Smith", "jane@example.com", 25, datetime