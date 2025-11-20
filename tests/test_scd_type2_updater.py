import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType
import datetime
from delta.tables import DeltaTable
import tempfile
import shutil
import os
from src.scd_type2_updater import update_scd_type2

class TestScdType2Updater(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestScdType2Updater") \
            .master("local[1]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create temporary directory for Delta tables
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog = "test_catalog"
        cls.schema = "test_schema"
        
        # Create sample customer data (initial)
        customer_data_initial = [
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
        
        cls.customer_df_initial = cls.spark.createDataFrame(customer_data_initial, schema=customer_schema)
        
        # Create sample customer data (updated)
        customer_data_updated = [
            ("C001", "John Doe Updated", "john_new@example.com", "North"),  # Name and email changed
            ("C002", "Jane Smith", "jane@example.com", "West"),  # Region changed
            ("C003", "Bob Johnson", "bob@example.com", "East")   # No change
        ]
        
        cls.customer_df_updated = cls.spark.createDataFrame(customer_data_updated, schema=customer_schema)
        
        # Create sample ordersummary data
        ordersummary_data = [
            ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1000.0, 2, 
             datetime.date(2023, 1, 15), 2000.0, True, datetime.datetime.now(), None),
            ("C002", "Jane Smith", "jane@example.com", "South", "O002", "Phone", 500.0, 1, 
             datetime.date(2023, 2, 20), 500.0, True, datetime.datetime.now(), None),
            ("C003", "Bob Johnson", "bob@example.com", "East", "O003", "Tablet", 300.0, 3, 
             datetime.date(2023, 3, 10), 900.0, True, datetime.datetime.now(), None)
        ]
        
        ordersummary_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("TotalAmount", DoubleType(), True),
            StructField("IsActive", BooleanType(), True),
            StructField("StartDate", TimestampType(), True),
            StructField("EndDate", TimestampType(), True)
        ])
        
        cls.ordersummary_df = cls.spark.createDataFrame(ordersummary_data, schema=ordersummary_schema)
        
        # Create the tables
        cls.customer_path = os.path.join(cls.temp_dir, "customer")
        cls.ordersummary_path = os.path.join(cls.temp_dir, "ordersummary")
        cls.customeraggregatespend_path = os.path.join(cls.temp_dir, "customeraggregatespend")
        
        # Create the tables
        cls.customer_df_initial.write.format("delta").save(cls.customer_path)
        cls.ordersummary_df.write.format("delta").save(cls.ordersummary_path)
        
        # Create the views/tables
        cls.spark.sql(f"CREATE DATABASE IF NOT EXISTS {cls.catalog}.{cls.schema}")
        cls.spark.sql(f"CREATE TABLE IF NOT EXISTS {cls.catalog}.{cls.schema}.customer USING DELTA LOCATION '{cls.customer_path}'")
        cls.spark.sql(f"CREATE TABLE IF NOT EXISTS {cls.catalog}.{cls.schema}.ordersummary USING DELTA LOCATION '{cls.ordersummary_path}'")
        cls.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cls.catalog}.{cls.schema}.customeraggregatespend (
            Name STRING,
            TotalAmount DOUBLE,
            Date DATE
        )
        USING DELTA
        LOCATION '{cls.customeraggregatespend_path}'
        """)
    
    @classmethod
    def tearDownClass(cls):
        # Clean up temporary directory
        shutil.rmtree(cls.temp_dir)
        cls.spark.stop()
    
    def test_update_scd_type2(self):
        # Update the customer table with new data
        self.customer_df_updated.write.format("delta").mode("overwrite").save(self.customer_path)
        
        # Run the SCD Type 2 update
        update_scd_type2(self.spark, self.catalog, self.schema)
        
        # Check the ordersummary table
        updated_ordersummary = self.spark.read.format("delta").load(self.ordersummary_path)
        
        # Count active records
        active_records = updated_ordersummary.filter("IsActive = true").count()
        self.assertEqual(active_records, 3)  # Should still have 3 active records
        
        # Check if John's record was updated
        john_records = updated_ordersummary.filter("CustId = 'C001'").count()
        self.assertEqual(john_records, 2)  # Should have 2 records for John (old and new)
        
        john_active = updated_ordersummary.filter("CustId = 'C001' AND IsActive = true").first()
        self.assertEqual(john_active["Name"], "John Doe Updated")  # Should have updated name
        
        # Check if Jane's record was updated
        jane_records = updated_ordersummary.filter("CustId = 'C002'").count()
        self.assertEqual(jane_records, 2)  # Should have 2 records for Jane (old and new)
        
        jane_active = updated_ordersummary.filter("CustId = 'C002' AND IsActive = true").first()
        self.assertEqual(jane_active["Region"], "West")  # Should have updated region
        
        # Check if Bob's record was not updated (no changes)
        bob_records = updated_ordersummary.filter("CustId = 'C003'").count()
        self.assertEqual(bob_records, 1)  # Should have only 1 record for Bob (no changes)

if __name__ == "__main__":
    unittest.main()