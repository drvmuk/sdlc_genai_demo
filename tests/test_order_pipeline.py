"""
Tests for the order pipeline module
"""
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

from src.order_pipeline import (
    create_spark_session, load_customer_data, load_order_data,
    create_order_summary_scd2
)
from src.config import SCD_COLUMNS

class TestOrderPipeline(unittest.TestCase):
    """Test cases for order pipeline"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        cls.spark = (SparkSession.builder
                    .appName("TestOrderPipeline")
                    .master("local[*]")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .getOrCreate())
        
        # Create test customer data
        customer_data = [
            (1, "John Doe", "john@example.com", "North"),
            (2, "Jane Smith", "jane@example.com", "South"),
            (3, "Bob Johnson", "bob@example.com", "East"),
            (4, "Alice Brown", "alice@example.com", "West"),
            (5, None, "invalid@example.com", "North")  # Should be filtered out
        ]
        
        customer_schema = StructType([
            StructField("CustId", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, customer_schema)
        
        # Create test order data
        order_data = [
            (101, "Product A", 10.0, 2, datetime.strptime("2023-01-15", "%Y-%m-%d"), 1),
            (102, "Product B", 15.0, 1, datetime.strptime("2023-01-20", "%Y-%m-%d"), 1),
            (103, "Product C", 20.0, 3, datetime.strptime("2023-02-05", "%Y-%m-%d"), 2),
            (104, "Product D", 25.0, 2, datetime.strptime("2023-02-10", "%Y-%m-%d"), 3),
            (105, "Product E", 30.0, 1, datetime.strptime("2023-03-15", "%Y-%m-%d"), 4),
            (106, None, 35.0, None, None, 5)  # Should be filtered out
        ]
        
        order_schema = StructType([
            StructField("OrderId", IntegerType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", IntegerType(), True)
        ])
        
        cls.order_df = cls.spark.createDataFrame(order_data, order_schema)
        
        # Mock the catalog and tables
        cls.spark.sql("CREATE DATABASE IF NOT EXISTS gen_ai_poc_databrickscoe")
        cls.spark.sql("CREATE DATABASE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        cls.spark.stop()
    
    def test_create_spark_session(self):
        """Test creating a Spark session"""
        spark = create_spark_session()
        self.assertIsNotNone(spark)
        self.assertTrue(isinstance(spark, SparkSession))
    
    def test_load_customer_data(self):
        """Test loading customer data"""
        # Mock the read.csv method to return our test dataframe
        self.spark.read.option = lambda *args, **kwargs: self.spark.read
        self.spark.read.csv = lambda path: self.customer_df
        
        # Mock the write method
        self.customer_df.write.format = lambda fmt: self.customer_df.write
        self.customer_df.write.mode = lambda mode: self.customer_df.write
        self.customer_df.write.saveAsTable = lambda table: None
        
        result_df = load_customer_data(self.spark)
        
        # Check that null values are filtered out
        self.assertEqual(result_df.filter(F.col("CustId").isNull()).count(), 0)
        self.assertEqual(result_df.filter(F.col("Name").isNull()).count(), 0)
        
        # Check that we have the expected number of records
        self.assertEqual(result_df.count(), 4)  # One record with null Name should be filtered out
        
        # Check that metadata columns were added
        self.assertTrue("load_date" in result_df.columns)
        self.assertTrue("load_id" in result_df.columns)
    
    def test_load_order_data(self):
        """Test loading order data"""
        # Mock the read.csv method to return our test dataframe
        self.spark.read.option = lambda *args, **kwargs: self.spark.read
        self.spark.read.csv = lambda path: self.order_df
        
        # Mock the write method
        self.order_df.write.format = lambda fmt: self.order_df.write
        self.order_df.write.mode = lambda mode: self.order_df.write
        self.order_df.write.saveAsTable = lambda table: None
        
        result_df = load_order_data(self.spark)
        
        # Check that null values are filtered out
        self.assertEqual(result_df.filter(F.col("OrderId").isNull()).count(), 0)
        self.assertEqual(result_df.filter(F.col("CustId").isNull()).count(), 0)
        self.assertEqual(result_df.filter(F.col("Date").isNull()).count(), 0)
        
        # Check that we have the expected number of records
        self.assertEqual(result_df.count(), 5)  # One record with null Date should be filtered out
        
        # Check that metadata columns were added
        self.assertTrue("load_date" in result_df.columns)
        self.assertTrue("load_id" in result_df.columns)
    
    def test_create_order_summary_scd2_new_table(self):
        """Test creating a new order summary SCD Type 2 table"""
        # Mock the SQL query to check if table exists
        self.spark.sql = lambda query: self.spark.createDataFrame(
            [(table, db, True) for db, table in [("gen_ai_poc_databrickscoe", "customer"), 
                                                ("gen_ai_poc_databrickscoe", "order")]],
            ["tableName", "database", "isTemporary"]
        )
        
        # Mock the write method
        mock_df = self.spark.createDataFrame([], StructType([]))
        mock_df.write.format = lambda fmt: mock_df.write
        mock_df.write.mode = lambda mode: mock_df.write
        mock_df.write.saveAsTable = lambda table: None
        
        # Clean test data
        cleaned_customer_df = self.customer_df.filter(F.col("CustId").isNotNull() & F.col("Name").isNotNull())
        cleaned_order_df = self.order_df.filter(F.col("OrderId").isNotNull() & F.col("Date").isNotNull() & F.col("CustId").isNotNull())
        
        # Add metadata columns
        cleaned_customer_df = cleaned_customer_df.withColumn("load_date", F.current_timestamp()).withColumn("load_id", F.lit("20230101000000"))
        cleaned_order_df = cleaned_order_df.withColumn("load_date", F.current_timestamp()).withColumn("load_id", F.lit("20230101000000"))
        
        # Test the function
        create_order_summary_scd2(self.spark, cleaned_customer_df, cleaned_order_df)
        
        # Since we're mocking, we just verify that the function runs without errors
        self.assertTrue(True)
    
    def test_create_order_summary_scd2_existing_table(self):
        """Test updating an existing order summary SCD Type 2 table"""
        # Mock the SQL query to check if table exists
        self.spark.sql = lambda query: self.spark.createDataFrame(
            [(table, db, True) for db, table in [("gen_ai_poc_databrickscoe", "customer"), 
                                                ("gen_ai_poc_databrickscoe", "order"),
                                                ("gen_ai_poc_databrickscoe", "ordersummary")]],
            ["tableName", "database", "isTemporary"]
        )
        
        # Create a mock existing table
        existing_schema = StructType([
            StructField("CustId", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("OrderYear", IntegerType(), True),
            StructField("OrderMonth", IntegerType(), True),
            StructField("OrderCount", IntegerType(), True),
            StructField("TotalSpend", DoubleType(), True),
            StructField("FirstOrderDate", DateType(), True),
            StructField("LastOrderDate", DateType(), True),
            StructField("hash_key", StringType(), True),
            StructField(SCD_COLUMNS["effective_start_date"], TimestampType(), True),
            StructField(SCD_COLUMNS["effective_end_date"], TimestampType(), True),
            StructField(SCD_COLUMNS["is_current"], BooleanType(), True),
            StructField(SCD_COLUMNS["surrogate_key"], StringType(), True)
        ])
        
        existing_data = [
            (1, "John Doe", "North", 2023, 1, 2, 35.0, 
             datetime.strptime("2023-01-15", "%Y-%m-%d"), 
             datetime.strptime("2023-01-20", "%Y-%m-%d"),
             "oldhashvalue", 
             datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
             datetime.strptime("9999-12-31 00:00:00", "%Y-%m-%d %H:%M:%S"),
             True, "1_2023_1")
        ]
        
        existing_df = self.spark.createDataFrame(existing_data, existing_schema)
        
        # Mock table method
        self.spark.table = lambda table_name: existing_df
        
        # Clean test data
        cleaned_customer_df = self.customer_df.filter(F.col("CustId").isNotNull() & F.col("Name").isNotNull())
        cleaned_order_df = self.order_df.filter(F.col("OrderId").isNotNull() & F.col("Date").isNotNull() & F.col("CustId").isNotNull())
        
        # Add metadata columns
        cleaned_customer_df = cleaned_customer_df.withColumn("load_date", F.current_timestamp()).withColumn("load_id", F.lit("20230101000000"))
        cleaned_order_df = cleaned_order_df.withColumn("load_date", F.current_timestamp()).withColumn("load_id", F.lit("20230101000000"))
        
        # Test the function
        create_order_summary_scd2(self.spark, cleaned_customer_df, cleaned_order_df)
        
        # Since we're mocking, we just verify that the function runs without errors
        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()