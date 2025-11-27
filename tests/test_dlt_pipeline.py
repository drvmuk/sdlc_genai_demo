import unittest
from pyspark.sql import SparkSession
import sys
import os
import tempfile
import shutil
from pyspark.sql import functions as F

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Note: This test simulates DLT functionality but doesn't actually run DLT
# since that requires a Databricks environment

class TestDLTPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Create a Spark session for testing."""
        cls.spark = SparkSession.builder \
            .appName("TestDLTPipeline") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create temporary directories for test data
        cls.temp_dir = tempfile.mkdtemp()
        cls.customer_data_path = os.path.join(cls.temp_dir, "customer_data")
        cls.order_data_path = os.path.join(cls.temp_dir, "order_data")
        
        # Create sample customer data
        customer_data = [
            (1, "John Doe", "john@example.com", "North"),
            (2, "Jane Smith", "jane@example.com", "South")
        ]
        customer_schema = ["CustId", "Name", "EmailId", "Region"]
        customer_df = cls.spark.createDataFrame(customer_data, customer_schema)
        customer_df.write.csv(cls.customer_data_path, header=True, mode="overwrite")
        
        # Create sample order data
        order_data = [
            (101, "Item1", 10.0, 2, "2023-01-01", 1),
            (102, "Item2", 15.0, 1, "2023-01-02", 2)
        ]
        order_schema = ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
        order_df = cls.spark.createDataFrame(order_data, order_schema)
        order_df.write.csv(cls.order_data_path, header=True, mode="overwrite")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up temporary directories and stop the Spark session."""
        shutil.rmtree(cls.temp_dir)
        cls.spark.stop()
    
    def test_data_transformation(self):
        """Test data transformations that would be done in DLT."""
        # Read customer data
        customer_df = self.spark.read.csv(self.customer_data_path, header=True, inferSchema=True)
        
        # Read order data
        order_df = self.spark.read.csv(self.order_data_path, header=True, inferSchema=True)
        
        # Add TotalAmount column to order data
        order_df = order_df.withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        
        # Join customer and order data
        joined_df = customer_df.join(order_df, "CustId", "inner")
        
        # Check that the join worked correctly
        self.assertEqual(joined_df.count(), 2)
        
        # Check that TotalAmount is calculated correctly
        order1 = joined_df.filter(F.col("OrderId") == 101).first()
        self.assertEqual(order1["TotalAmount"], 20.0)  # 10.0 * 2
        
        order2 = joined_df.filter(F.col("OrderId") == 102).first()
        self.assertEqual(order2["TotalAmount"], 15.0)  # 15.0 * 1
        
        # Test aggregation for customeraggregatespend
        agg_df = joined_df.groupBy("Name", "Date").agg(F.sum("TotalAmount").alias("TotalAmount"))
        
        # Check aggregation results
        self.assertEqual(agg_df.count(), 2)
        
        john_spend = agg_df.filter(F.col("Name") == "John Doe").first()
        self.assertEqual(john_spend["TotalAmount"], 20.0)
        
        jane_spend = agg_df.filter(F.col("Name") == "Jane Smith").first()
        self.assertEqual(jane_spend["TotalAmount"], 15.0)

if __name__ == "__main__":
    unittest.main()