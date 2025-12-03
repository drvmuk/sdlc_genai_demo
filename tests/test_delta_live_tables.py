import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import os
import tempfile
import shutil

# Note: Testing DLT pipelines is challenging in a unit test environment
# This test file provides a simplified approach to test the transformations
# but not the actual DLT execution which requires a Databricks environment

class TestDeltaLiveTables(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("TestDeltaLiveTables") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create temporary directories for test data
        cls.temp_dir = tempfile.mkdtemp()
        cls.customer_data_path = os.path.join(cls.temp_dir, "customerdata")
        cls.order_data_path = os.path.join(cls.temp_dir, "orderdata")
        
        os.makedirs(cls.customer_data_path)
        os.makedirs(cls.order_data_path)
        
        # Define schemas
        cls.customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        # Create sample data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C004", None, "alice@example.com", "West"),
            ("C001", "John Doe", "john@example.com", "North")  # Duplicate
        ]
        
        order_data = [
            ("O001", "Laptop", 1000.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Phone", 500.0, 1, datetime.date(2023, 1, 20), "C002"),
            ("O003", "Tablet", 300.0, 3, datetime.date(2023, 1, 25), "C003"),
            ("O004", "Headphones", 100.0, None, datetime.date(2023, 1, 30), "C001"),
            ("O002", "Phone", 500.0, 1, datetime.date(2023, 1, 20), "C002")  # Duplicate
        ]
        
        # Create DataFrames
        cls.customer_df = cls.spark.createDataFrame(customer_data, cls.customer_schema)
        cls.order_df = cls.spark.createDataFrame(order_data, cls.order_schema)
        
        # Write sample data to temporary files
        cls.customer_df.write.format("csv").option("header", "true").mode("overwrite").save(cls.customer_data_path)
        cls.order_df.write.format("csv").option("header", "true").mode("overwrite").save(cls.order_data_path)
    
    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()
        
        # Clean up temporary directories
        shutil.rmtree(cls.temp_dir)
    
    def test_dlt_transformations(self):
        # This is a simplified test to verify the transformations that would be applied in DLT
        # In a real environment, we would test the actual DLT pipeline execution
        
        # Test customer_bronze transformation
        customer_bronze = self.spark.read.format("csv").option("header", "true").load(self.customer_data_path)
        self.assertEqual(customer_bronze.count(), 5)
        
        # Test order_bronze transformation
        order_bronze = self.spark.read.format("csv").option("header", "true").load(self.order_data_path)
        self.assertEqual(order_bronze.count(), 5)
        
        # Test customer_silver transformation (clean customer data)
        customer_silver = customer_bronze.dropDuplicates().na.drop()
        self.assertEqual(customer_silver.count(), 3)  # 5 original - 1 duplicate - 1 with null
        
        # Test order_silver transformation (clean order data and add TotalAmount)
        from pyspark.sql.functions import col
        order_silver = order_bronze.dropDuplicates().na.drop() \
            .withColumn("PricePerUnit", col("PricePerUnit").cast("double")) \
            .withColumn("Qty", col("Qty").cast("integer")) \
            .withColumn("Date", col("Date").cast("date")) \
            .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        
        self.assertEqual(order_silver.count(), 3)  # 5 original - 1 duplicate - 1 with null
        
        # Test ordersummary transformation (join customer and order data)
        ordersummary = customer_silver.join(order_silver, "CustId")
        self.assertTrue(ordersummary.count() > 0)
        
        # Test customeraggregatespend transformation (aggregate TotalAmount by Name and Date)
        from pyspark.sql.functions import sum as sum_
        customeraggregatespend = ordersummary.groupBy("Name", "Date").agg(sum_("TotalAmount").alias("TotalAmount"))
        self.assertTrue(customeraggregatespend.count() > 0)

if __name__ == "__main__":
    unittest.main()