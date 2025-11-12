"""
Tests for Delta Live Tables implementation.
"""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime


class TestDeltaLiveTables(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDeltaLiveTables") \
            .master("local[1]") \
            .getOrCreate()
        
        # Create test data for customers
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East")
        ]
        
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, customer_schema)
        
        # Create test data for orders
        order_data = [
            ("O001", "Item1", 10.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Item2", 15.0, 3, datetime.date(2023, 1, 16), "C002"),
            ("O003", "Item3", 20.0, 1, datetime.date(2023, 1, 17), "C003")
        ]
        
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        cls.order_df = cls.spark.createDataFrame(order_data, order_schema)
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    def test_join_and_calculate_total(self):
        """
        Test joining customer and order data and calculating total amount.
        This simulates what happens in the DLT pipeline.
        """
        # Join customer and order data
        joined_df = self.customer_df.join(
            self.order_df,
            on="CustId",
            how="inner"
        )
        
        # Calculate TotalAmount
        result_df = joined_df.withColumn("TotalAmount", self.order_df["PricePerUnit"] * self.order_df["Qty"])
        
        # Check that the join worked correctly
        self.assertEqual(result_df.count(), 3)
        
        # Check that TotalAmount is calculated correctly
        row1 = result_df.filter(result_df.OrderId == "O001").collect()[0]
        self.assertEqual(row1.TotalAmount, 20.0)
        
        row2 = result_df.filter(result_df.OrderId == "O002").collect()[0]
        self.assertEqual(row2.TotalAmount, 45.0)
        
        row3 = result_df.filter(result_df.OrderId == "O003").collect()[0]
        self.assertEqual(row3.TotalAmount, 20.0)


if __name__ == "__main__":
    unittest.main()