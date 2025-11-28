import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class TestDLTPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDLTPipeline") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create sample data
        # Customer data with schema
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C004", "Alice Brown", "alice@example.com", "West"),
            ("C005", None, "mike@example.com", "North"),  # Null value
            ("C001", "John Doe", "john@example.com", "North")  # Duplicate
        ]
        
        cls.customer_df = cls.spark.createDataFrame(cls.customer_data, customer_schema)
        
        # Order data with schema
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        cls.order_data = [
            ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Phone", 800.0, 2, datetime.date(2023, 1, 20), "C002"),
            ("O003", "Headphones", 100.0, 3, datetime.date(2023, 2, 5), "C003"),
            ("O004", "Monitor", 300.0, 1, datetime.date(2023, 2, 10), "C004"),
            ("O005", "Keyboard", 50.0, None, datetime.date(2023, 3, 1), "C001"),  # Null value
            ("O001", "Laptop", 1200.0, 1, datetime.date(2023, 1, 15), "C001")  # Duplicate
        ]
        
        cls.order_df = cls.spark.createDataFrame(cls.order_data, order_schema)
        
        # Create temp views for testing
        cls.customer_df.createOrReplaceTempView("customer_bronze_view")
        cls.order_df.createOrReplaceTempView("order_bronze_view")
    
    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()
    
    def test_customer_silver_transformation(self):
        # Test the customer silver transformation logic
        
        # Apply the transformation logic that would be in the DLT pipeline
        customer_silver_df = self.customer_df.na.drop().dropDuplicates()
        
        # Verify the results
        self.assertEqual(customer_silver_df.count(), 4)
        self.assertEqual(customer_silver_df.filter(customer_silver_df.CustId == "C001").count(), 1)
    
    def test_order_silver_transformation(self):
        # Test the order silver transformation logic
        
        # Apply the transformation logic that would be in the DLT pipeline
        order_silver_df = self.order_df.na.drop().dropDuplicates() \
            .withColumn("TotalAmount", self.order_df["PricePerUnit"] * self.order_df["Qty"])
        
        # Verify the results
        self.assertEqual(order_silver_df.count(), 4)  # Removed null and duplicate
        self.assertTrue("TotalAmount" in order_silver_df.columns)
        
        # Check calculation
        row = order_silver_df.filter(order_silver_df.OrderId == "O002").collect()[0]
        self.assertEqual(row.TotalAmount, 1600.0)  # 800.0 * 2 = 1600.0
    
    def test_customeraggregatespend_logic(self):
        # Test the aggregate spend calculation logic
        
        # Create a mock ordersummary dataframe with IsActive column
        from pyspark.sql.functions import lit
        
        # Join customer and order data
        joined_df = self.customer_df.join(
            self.order_df.na.drop().dropDuplicates().withColumn("TotalAmount", 
                self.order_df["PricePerUnit"] * self.order_df["Qty"]),
            "CustId", 
            "inner"
        ).withColumn("IsActive", lit(True))
        
        # Apply the aggregation logic
        from pyspark.sql.functions import sum as spark_sum
        aggregated_df = joined_df.filter(joined_df["IsActive"] == True) \
            .groupBy("Name", "Date") \
            .agg(spark_sum("TotalAmount").alias("TotalAmount"))
        
        # Verify results
        self.assertEqual(aggregated_df.count(), 4)  # One row per customer with valid order
        
        # Check specific aggregation
        john_row = aggregated_df.filter(aggregated_df.Name == "John Doe").collect()[0]
        self.assertEqual(john_row.TotalAmount, 1200.0)  # Only one valid order for John

if __name__ == "__main__":
    unittest.main()