import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import sys
import os

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.data_processor import clean_data

class TestDataProcessor(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestCustomerOrderDataProcessor") \
            .master("local[*]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    def test_clean_data(self):
        # Create a test dataframe with null and duplicate values
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        test_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", None, "bob@example.com", "East"),
            ("C004", "Alice Brown", "alice@example.com", "West"),
            ("C005", "Tom Wilson", "Null", "North"),
            ("C001", "John Doe", "john@example.com", "North")  # Duplicate
        ]
        
        test_df = self.spark.createDataFrame(test_data, customer_schema)
        
        # Apply clean_data function
        cleaned_df = clean_data(test_df)
        
        # Check results
        self.assertEqual(cleaned_df.count(), 3)  # Should have 3 valid rows
        
        # Check that no nulls remain
        self.assertEqual(cleaned_df.filter(cleaned_df.Name.isNull()).count(), 0)
        
        # Check that no "Null" string values remain
        self.assertEqual(cleaned_df.filter(cleaned_df.EmailId == "Null").count(), 0)
        
        # Check that duplicates were removed
        self.assertEqual(cleaned_df.filter(cleaned_df.CustId == "C001").count(), 1)

    def test_clean_data_with_empty_df(self):
        # Test with empty dataframe
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        empty_df = self.spark.createDataFrame([], customer_schema)
        cleaned_empty_df = clean_data(empty_df)
        
        # Should still be empty
        self.assertEqual(cleaned_empty_df.count(), 0)

if __name__ == "__main__":
    unittest.main()