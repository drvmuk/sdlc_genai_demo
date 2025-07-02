"""
Unit tests for the OrderSummaryProcessor class.
"""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import datetime
import os
import tempfile
import shutil
from src.order_summary_processor import OrderSummaryProcessor

class TestOrderSummaryProcessor(unittest.TestCase):
    """Test cases for OrderSummaryProcessor."""

    @classmethod
    def setUpClass(cls):
        """Set up SparkSession and test data."""
        cls.spark = SparkSession.builder \
            .appName("TestOrderSummaryProcessor") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Create test directories
        cls.test_dir = tempfile.mkdtemp()
        cls.customer_dir = os.path.join(cls.test_dir, "customer")
        cls.order_dir = os.path.join(cls.test_dir, "order")
        os.makedirs(cls.customer_dir, exist_ok=True)
        os.makedirs(cls.order_dir, exist_ok=True)
        
        # Create test data
        cls.create_test_data()
        
        # Initialize processor
        cls.processor = OrderSummaryProcessor(cls.spark)

    @classmethod
    def tearDownClass(cls):
        """Clean up resources."""
        cls.spark.stop()
        shutil.rmtree(cls.test_dir)

    @classmethod
    def create_test_data(cls):
        """Create test data for customers and orders."""
        # Customer data
        customer_data = [
            ("C001", "John", "Doe", "123 Main St", "New York", "NY", "10001"),
            ("C002", "Jane", "Smith", "456 Oak Ave", "Los Angeles", "CA", "90001"),
            ("C003", "Null", "Null", "Null", "Null", "Null", "Null"),  # Should be filtered out
            ("C004", "Bob", "Johnson", "789 Pine Rd", "Chicago", "IL", "60007"),
            ("C004", "Bob", "Johnson", "789 Pine Rd", "Chicago", "IL", "60007"),  # Duplicate
        ]
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
            StructField("ZipCode", StringType(), True)
        ])
        customer_df = cls.spark.createDataFrame(customer_data, customer_schema)
        customer_df.write.csv(cls.customer_dir, header=True, mode="overwrite")
        
        # Order data
        order_data = [
            ("O001", "C001", 100.50, "2023-01-15", "Completed"),
            ("O002", "C002", 200.75, "2023-01-20", "Pending"),
            ("O003", "C001", 50.25, "2023-01-25", "Completed"),
            ("O004", "Null", 300.00, "2023-01-30", "Null"),  # Should be filtered out
            ("O005", "C004", 150.00, "2023-02-01", "Completed"),
            ("O005", "C004", 150.00, "2023-02-01", "Completed"),  # Duplicate
        ]
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("CustId", StringType(), True),
            StructField("Amount", IntegerType(), True),
            StructField("OrderDate", StringType(), True),
            StructField("Status", StringType(), True)
        ])
        order_df = cls.spark.createDataFrame(order_data, order_schema)
        order_df.write.csv(cls.order_dir, header=True, mode="overwrite")

    def test_read_csv_data(self):
        """Test reading CSV data."""
        customer_df = self.processor.read_csv_data(self.customer_dir, "customer data")
        self.assertIsNotNone(customer_df)
        self.assertTrue("CustId" in customer_df.columns)
        self.assertEqual(customer_df.count(), 5)  # Including the null and duplicate records
        
        order_df = self.processor.read_csv_data(self.order_dir, "order data")
        self.assertIsNotNone(order_df)
        self.assertTrue("OrderId" in order_df.columns)
        self.assertEqual(order_df.count(), 6)  # Including the null and duplicate records

    def test_clean_data(self):
        """Test cleaning data (removing nulls and duplicates)."""
        customer_df = self.processor.read_csv_data(self.customer_dir, "customer data")
        clean_customer_df = self.processor.clean_data(customer_df, "customer")
        self.assertEqual(clean_customer_df.count(), 3)  # 5 original - 1 null - 1 duplicate = 3
        
        order_df = self.processor.read_csv_data(self.order_dir, "order data")
        clean_order_df = self.processor.clean_data(order_df, "order")
        self.assertEqual(clean_order_df.count(), 4)  # 6 original - 1 null - 1 duplicate = 4

    def test_join_data(self):
        """Test joining customer and order data."""
        customer_df = self.processor.read_csv_data(self.customer_dir, "customer data")
        order_df = self.processor.read_csv_data(self.order_dir, "order data")
        
        clean_customer_df = self.processor.clean_data(customer_df, "customer")
        clean_order_df = self.processor.clean_data(order_df, "order")
        
        joined_df = self.processor.join_data(clean_customer_df, clean_order_df)
        self.assertEqual(joined_df.count(), 4)  # 3 orders for valid customers (C001, C002, C004)

    def test_load_to_delta_table(self):
        """Test loading data to Delta table."""
        customer_df = self.processor.read_csv_data(self.customer_dir, "customer data")
        clean_customer_df = self.processor.clean_data(customer_df, "customer")
        
        # Create a temporary table name
        temp_table = "temp_customer_table"
        
        # Load data to Delta table
        self.processor.load_to_delta_table(clean_customer_df, temp_table, "overwrite")
        
        # Verify the table was created and has the correct data
        result_df = self.spark.table(temp_table)
        self.assertEqual(result_df.count(), 3)
        self.assertTrue("CustId" in result_df.columns)
        
        # Clean up
        self.spark.sql(f"DROP TABLE IF EXISTS {temp_table}")

    def test_update_scd_type2_new_table(self):
        """Test updating SCD Type 2 table when it doesn't exist yet."""
        customer_df = self.processor.read_csv_data(self.customer_dir, "customer data")
        order_df = self.processor.read_csv_data(self.order_dir, "order data")
        
        clean_customer_df = self.processor.clean_data(customer_df, "customer")
        clean_order_df = self.processor.clean_data(order_df, "order")
        
        joined_df = self.processor.join_data(clean_customer_df, clean_order_df)
        
        # Create a temporary table name
        temp_table = "temp_scd_table"
        
        # Update SCD Type 2 table (should create it since it doesn't exist)
        self.processor.update_scd_type2(joined_df, temp_table)
        
        # Verify the table was created with SCD Type 2 columns
        result_df = self.spark.table(temp_table)
        self.assertEqual(result_df.count(), 4)
        self.assertTrue("StartDate" in result_df.columns)
        self.assertTrue("EndDate" in result_df.columns)
        self.assertTrue("IsActive" in result_df.columns)
        
        # All records should be active
        active_count = result_df.filter("IsActive = true").count()
        self.assertEqual(active_count, 4)
        
        # Clean up
        self.spark.sql(f"DROP TABLE IF EXISTS {temp_table}")

    def test_end_to_end_process(self):
        """Test the end-to-end process."""
        # Create a temporary table name
        temp_table = "temp_order_summary"
        
        # Run the process
        result = self.processor.process(self.customer_dir, self.order_dir, temp_table)
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify the final table
        result_df = self.spark.table(temp_table)
        self.assertTrue(result_df.count() > 0)
        self.assertTrue("StartDate" in result_df.columns)
        self.assertTrue("EndDate" in result_df.columns)
        self.assertTrue("IsActive" in result_df.columns)
        
        # Clean up
        self.spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
        self.spark.sql("DROP TABLE IF EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customer")
        self.spark.sql("DROP TABLE IF EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.order")


if __name__ == "__main__":
    unittest.main()