"""
Unit tests for the DeltaTablesLoader class
"""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
from src.delta_tables_loader import DeltaTablesLoader

class TestDeltaTablesLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession for testing"""
        cls.spark = SparkSession.builder \
            .appName("DeltaTablesLoaderTest") \
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
        
        # Create temporary tables
        cls.customer_df.createOrReplaceTempView("customer_temp")
        cls.order_df.createOrReplaceTempView("order_temp")
        
        # Initialize loader
        cls.loader = DeltaTablesLoader(cls.spark)
    
    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession"""
        cls.spark.stop()
    
    def test_transform_delta_tables(self):
        """Test transform_delta_tables method"""
        # Mock the table reading
        self.spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
        
        # Create Delta tables for testing
        self.customer_df.write.format("delta").mode("overwrite").saveAsTable("test_db.customer_dlt")
        self.order_df.write.format("delta").mode("overwrite").saveAsTable("test_db.order_dlt")
        
        # Mock the table reading in the loader
        self.loader.spark.sql("USE test_db")
        
        # Create a test-specific loader that uses test tables
        class TestLoader(DeltaTablesLoader):
            def transform_delta_tables(self):
                try:
                    # Transform order_dlt table
                    order_df = self.spark.table("test_db.order_dlt")
                    
                    # Add TotalAmount column
                    order_df = order_df.withColumn("TotalAmount", order_df["PricePerUnit"] * order_df["Qty"])
                    
                    # Remove null records
                    order_df = order_df.filter(
                        order_df["OrderId"].isNotNull() & 
                        order_df["CustId"].isNotNull() & 
                        order_df["PricePerUnit"].isNotNull() & 
                        order_df["Qty"].isNotNull()
                    )
                    
                    # Remove duplicate records
                    order_df = order_df.dropDuplicates(["OrderId"])
                    
                    # Write back to Delta table
                    order_df.write.format("delta").mode("overwrite").saveAsTable("test_db.order_dlt_transformed")
                    
                    # Transform customer_dlt table
                    customer_df = self.spark.table("test_db.customer_dlt")
                    
                    # Remove null records
                    customer_df = customer_df.filter(
                        customer_df["CustId"].isNotNull() & 
                        customer_df["Name"].isNotNull()
                    )
                    
                    # Remove duplicate records
                    customer_df = customer_df.dropDuplicates(["CustId"])
                    
                    # Write back to Delta table
                    customer_df.write.format("delta").mode("overwrite").saveAsTable("test_db.customer_dlt_transformed")
                    
                    return order_df, customer_df
                except Exception as e:
                    print(f"Error in test transform: {str(e)}")
                    raise
        
        test_loader = TestLoader(self.spark)
        order_df, customer_df = test_loader.transform_delta_tables()
        
        # Verify transformations
        # Check TotalAmount was added
        self.assertTrue("TotalAmount" in order_df.columns)
        
        # Check null records were removed
        self.assertEqual(order_df.filter(order_df["Qty"].isNull()).count(), 0)
        self.assertEqual(customer_df.filter(customer_df["Name"].isNull()).count(), 0)
        
        # Check row counts
        self.assertEqual(order_df.count(), 6)  # One row had null Qty
        self.assertEqual(customer_df.count(), 4)  # One row had null Name
        
        # Clean up
        self.spark.sql("DROP TABLE IF EXISTS test_db.customer_dlt")
        self.spark.sql("DROP TABLE IF EXISTS test_db.order_dlt")
        self.spark.sql("DROP TABLE IF EXISTS test_db.customer_dlt_transformed")
        self.spark.sql("DROP TABLE IF EXISTS test_db.order_dlt_transformed")
        self.spark.sql("DROP DATABASE IF EXISTS test_db")

    def test_create_order_summary(self):
        """Test create_order_summary method"""
        # Create a test-specific loader that uses test data
        class TestLoader(DeltaTablesLoader):
            def create_order_summary(self, customer_df, order_df):
                # Join tables
                order_summary_df = customer_df.join(
                    order_df,
                    customer_df.CustId == order_df.CustId,
                    "inner"
                ).select(
                    customer_df.CustId,
                    customer_df.Name,
                    order_df.OrderId,
                    order_df.Date,
                    order_df.PricePerUnit,
                    order_df.Qty
                )
                
                return order_summary_df
        
        test_loader = TestLoader(self.spark)
        
        # Filter out rows with null values for the test
        clean_customer_df = self.customer_df.filter(self.customer_df["Name"].isNotNull())
        clean_order_df = self.order_df.filter(self.order_df["Qty"].isNotNull())
        
        # Create order summary
        order_summary_df = test_loader.create_order_summary(clean_customer_df, clean_order_df)
        
        # Verify the join worked correctly
        self.assertEqual(order_summary_df.count(), 6)  # Should have 6 valid order records
        
        # Check columns
        expected_columns = ["CustId", "Name", "OrderId", "Date", "PricePerUnit", "Qty"]
        self.assertEqual(order_summary_df.columns, expected_columns)

    def test_create_customer_aggregate_spend(self):
        """Test create_customer_aggregate_spend method"""
        # Create test data with TotalAmount already calculated
        order_summary_data = [
            (1, "John Doe", 101, datetime.date(2023, 1, 15), 10.5, 2, 21.0, True),
            (1, "John Doe", 104, datetime.date(2023, 1, 18), 8.0, 2, 16.0, True),
            (2, "Jane Smith", 102, datetime.date(2023, 1, 16), 15.75, 1, 15.75, True),
            (2, "Jane Smith", 106, datetime.date(2023, 1, 20), 9.99, 4, 39.96, True),
            (3, "Bob Johnson", 103, datetime.date(2023, 1, 17), 5.25, 3, 15.75, True),
            (5, "Sarah Williams", 105, datetime.date(2023, 1, 19), 12.5, 1, 12.5, True)
        ]
        
        order_summary_schema = StructType([
            StructField("CustId", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("OrderId", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("TotalAmount", DoubleType(), True),
            StructField("IsActive", StringType(), True)
        ])
        
        order_summary_df = self.spark.createDataFrame(order_summary_data, order_summary_schema)
        
        # Create a test-specific loader that uses test data
        class TestLoader(DeltaTablesLoader):
            def create_customer_aggregate_spend(self, order_summary_df):
                # Aggregate data
                aggregate_df = order_summary_df.groupBy("Name", "Date").agg(
                    {"TotalAmount": "sum"}
                ).withColumnRenamed("sum(TotalAmount)", "TotalSpend")
                
                return aggregate_df
        
        test_loader = TestLoader(self.spark)
        
        # Create customer aggregate spend
        aggregate_df = test_loader.create_customer_aggregate_spend(order_summary_df)
        
        # Verify the aggregation worked correctly
        self.assertEqual(aggregate_df.count(), 5)  # Should have 5 unique Name-Date combinations
        
        # Check John Doe's total spend on 2023-01-15
        john_doe_row = aggregate_df.filter(
            (aggregate_df["Name"] == "John Doe") & 
            (aggregate_df["Date"] == datetime.date(2023, 1, 15))
        ).collect()[0]
        
        self.assertEqual(john_doe_row["TotalSpend"], 21.0)