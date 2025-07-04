"""
Unit tests for ETL processor
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import datetime
from src.etl_processor import ETLProcessor

class TestETLProcessor(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """
        Set up SparkSession for testing
        """
        cls.spark = SparkSession.builder \
            .appName("TestETLProcessor") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
            
        # Create test data
        # Customer data
        customer_data = [
            ("C001", "John Doe", "123 Main St", "555-1234"),
            ("C002", "Jane Smith", "456 Oak Ave", "555-5678"),
            ("C003", "Bob Johnson", "789 Pine Rd", "555-9012"),
            (None, "Invalid Customer", "No Address", "555-0000"),  # Will be dropped due to null
            ("C001", "John Doe", "123 Main St", "555-1234")  # Duplicate will be dropped
        ]
        
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("Phone", StringType(), True)
        ])
        
        # Order data
        order_data = [
            ("C001", "O001", datetime.datetime.now(), "100.00"),
            ("C002", "O002", datetime.datetime.now(), "200.00"),
            ("C003", "O003", datetime.datetime.now(), "150.00"),
            ("C001", "O004", datetime.datetime.now(), "75.00"),
            (None, "O005", datetime.datetime.now(), "50.00")  # Will be dropped due to null
        ]
        
        order_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("OrderId", StringType(), True),
            StructField("OrderDate", TimestampType(), True),
            StructField("Amount", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, customer_schema)
        cls.order_df = cls.spark.createDataFrame(order_data, order_schema)
        
        # Create a temporary view for testing
        cls.customer_df.createOrReplaceTempView("customer_data")
        cls.order_df.createOrReplaceTempView("order_data")
        
        # Initialize ETL processor
        cls.etl_processor = ETLProcessor(cls.spark)
    
    @classmethod
    def tearDownClass(cls):
        """
        Clean up resources
        """
        cls.spark.stop()
    
    def test_cleansing_removes_nulls_and_duplicates(self):
        """
        Test that cleansing removes null values and duplicates
        """
        # Mock the read operation to return our test data
        self.etl_processor.spark.read.option = lambda *args, **kwargs: self
        self.etl_processor.spark.read.csv = lambda path: self.customer_df if "customer" in path else self.order_df
        
        # Create a subclass to override the write operation
        class MockDataFrame:
            def __init__(self, df):
                self.df = df
                
            def format(self, fmt):
                return self
                
            def mode(self, mode):
                return self
                
            def save(self, path):
                return self.df
                
            def saveAsTable(self, table_name):
                return self.df
        
        # Override DataFrame.write to return our mock
        original_write = self.customer_df.write
        self.customer_df.write = lambda: MockDataFrame(self.customer_df)
        self.order_df.write = lambda: MockDataFrame(self.order_df)
        
        try:
            # Run the cleansing
            cleansed_customer_df, cleansed_order_df = self.etl_processor.ingest_and_cleanse_data()
            
            # Verify nulls and duplicates are removed
            self.assertEqual(cleansed_customer_df.count(), 3)  # 5 original - 1 null - 1 duplicate
            self.assertEqual(cleansed_order_df.count(), 4)     # 5 original - 1 null
            
            # Verify specific records
            customer_ids = [row.CustId for row in cleansed_customer_df.collect()]
            self.assertIn("C001", customer_ids)
            self.assertIn("C002", customer_ids)
            self.assertIn("C003", customer_ids)
            
        finally:
            # Restore original methods
            self.customer_df.write = original_write
            self.order_df.write = original_write
    
    def test_join_customer_and_order_data(self):
        """
        Test joining customer and order data
        """
        # Create a method to simulate the join operation
        def simulate_join():
            return self.customer_df.join(
                self.order_df,
                self.customer_df.CustId == self.order_df.CustId,
                "inner"
            )
        
        # Execute the join
        joined_data = simulate_join()
        
        # Verify the join result
        self.assertEqual(joined_data.count(), 4)  # 3 customers with 4 orders
        
        # Verify specific join conditions
        cust_order_pairs = [(row.CustId, row.OrderId) for row in joined_data.collect()]
        self.assertIn(("C001", "O001"), cust_order_pairs)
        self.assertIn(("C001", "O004"), cust_order_pairs)
        self.assertIn(("C002", "O002"), cust_order_pairs)
        self.assertIn(("C003", "O003"), cust_order_pairs)
    
    def test_scd_type2_logic(self):
        """
        Test SCD Type 2 logic for detecting changes
        """
        # Create current data
        current_data = [
            ("C001", "John Doe", "123 Main St", "555-1234", "O001", datetime.datetime.now(), "100.00", 
             datetime.datetime.now(), None, True),
            ("C002", "Jane Smith", "456 Oak Ave", "555-5678", "O002", datetime.datetime.now(), "200.00", 
             datetime.datetime.now(), None, True)
        ]
        
        current_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("Phone", StringType(), True),
            StructField("OrderId", StringType(), True),
            StructField("OrderDate", TimestampType(), True),
            StructField("Amount", StringType(), True),
            StructField("EffectiveDate", TimestampType(), True),
            StructField("EndDate", TimestampType(), True),
            StructField("IsCurrent", StringType(), True)
        ])
        
        current_df = self.spark.createDataFrame(current_data, current_schema)
        
        # Create new data with a change
        new_data = [
            ("C001", "John Doe", "123 Main St", "555-1234"),  # No change
            ("C002", "Jane Smith", "789 New Address", "555-5678")  # Address changed
        ]
        
        new_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("Phone", StringType(), True)
        ])
        
        new_df = self.spark.createDataFrame(new_data, new_schema)
        
        # Create a hash column to detect changes
        from pyspark.sql.functions import sha2, concat_ws, col
        
        current_with_hash = current_df.withColumn(
            "customer_hash",
            sha2(concat_ws("||", col("Name"), col("Address"), col("Phone")), 256)
        )
        
        new_with_hash = new_df.withColumn(
            "customer_hash",
            sha2(concat_ws("||", col("Name"), col("Address"), col("Phone")), 256)
        )
        
        # Join to find changed records
        joined = current_with_hash.join(
            new_with_hash,
            current_with_hash.CustId == new_with_hash.CustId,
            "inner"
        )
        
        # Identify changed records
        changed_records = joined.filter(
            joined["customer_hash"] != joined["customer_hash_1"]
        ).select(current_with_hash["CustId"])
        
        # Verify the changed record
        changed_ids = [row.CustId for row in changed_records.collect()]
        self.assertEqual(len(changed_ids), 1)
        self.assertEqual(changed_ids[0], "C002")

if __name__ == "__main__":
    unittest.main()