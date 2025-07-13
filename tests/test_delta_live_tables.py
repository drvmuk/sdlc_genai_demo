"""
Unit tests for the Delta Live Tables implementation
"""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime

class TestDeltaLiveTables(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession for testing"""
        cls.spark = SparkSession.builder \
            .appName("DeltaLiveTablesTest") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create test data
        # Customer data
        customer_schema = StructType([
            StructField("CustId", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Address", StringType(), True)
        ])
        
        customer_data = [
            (1, "John Doe", "123 Main St"),
            (2, "Jane Smith", "456 Oak Ave"),
            (3, "Bob Johnson", "789 Pine Rd"),
            (4, None, "101 Elm St"),  # Null name
            (5, "Sarah Williams", "202 Maple Dr")
        ]
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, customer_schema)
        
        # Order data
        order_schema = StructType([
            StructField("OrderId", IntegerType(), True),
            StructField("CustId", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True)
        ])
        
        order_data = [
            (101, 1, datetime.date(2023, 1, 15), 10.5, 2),
            (102, 2, datetime.date(2023, 1, 16), 15.75, 1),
            (103, 3, datetime.date(2023, 1, 17), 5.25, 3),
            (104, 1, datetime.date(2023, 1, 18), 8.0, 2),
            (105, 5, datetime.date(2023, 1, 19), 12.5, 1),
            (106, 2, datetime.date(2023, 1, 20), 9.99, 4),
            (107, 3, datetime.date(2023, 1, 21), 7.5, None)  # Null quantity
        ]
        
        cls.order_df = cls.spark.createDataFrame(order_data, order_schema)
    
    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession"""
        cls.spark.stop()
    
    def test_customer_dlt_transformation(self):
        """Test customer_dlt transformation logic"""
        # Simulate the DLT transformation
        result_df = (
            self.customer_df
            .dropDuplicates(["CustId"])
            .filter(self.customer_df["CustId"].isNotNull() & self.customer_df["Name"].isNotNull())
        )
        
        # Verify transformations
        self.assertEqual(result_df.count(), 4)  # One row had null Name
        self.assertEqual(result_df.filter(result_df["Name"].isNull()).count(), 0)
    
    def test_order_dlt_transformation(self):
        """Test order_dlt transformation logic"""
        # Simulate the DLT transformation
        result_df = (
            self.order_df
            .withColumn("TotalAmount", self.order_df["PricePerUnit"] * self.order_df["Qty"])
            .filter(
                self.order_df["OrderId"].isNotNull() & 
                self.order_df["CustId"].isNotNull() & 
                self.order_df["PricePerUnit"].isNotNull() & 
                self.order_df["Qty"].isNotNull()
            )
            .dropDuplicates(["OrderId"])
        )
        
        # Verify transformations
        self.assertEqual(result_df.count(), 6)  # One row had null Qty
        self.assertTrue("TotalAmount" in result_df.columns)
        
        # Check TotalAmount calculation
        first_row = result_df.filter(result_df["OrderId"] == 101).collect()[0]
        self.assertEqual(