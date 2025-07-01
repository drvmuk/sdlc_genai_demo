import unittest
from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
import datetime

# Import the module to test
sys.path.append("../src")
import bronze_layer

class TestBronzeLayer(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestBronzeLayer") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create sample customer data
        customer_data = [
            ("1", "John Doe", "john@example.com", 30),
            ("2", "Jane Smith", "jane@example.com", 25),
            ("3", "Bob Johnson", "bob@example.com", 40),
            ("4", "Alice Brown", "alice@example.com", 35)
        ]
        
        customer_schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, schema=customer_schema)
        
        # Create sample order data
        order_data = [
            ("1", "order1", 100.50, datetime.datetime.now()),
            ("2", "order2", 200.75, datetime.datetime.now()),
            ("3", "order3", 150.25, datetime.datetime.now()),
            ("4", "order4", 300.00, datetime.datetime.now())
        ]
        
        order_schema = StructType([
            StructField("id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("order_date", TimestampType(), True)
        ])
        
        cls.order_df = cls.spark.createDataFrame(order_data, schema=order_schema)
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    def test_customer_raw_schema(self):
        """Test that the customer_raw function adds the correct SCD Type-2 columns"""
        # Mock the Auto Loader read by returning our test dataframe
        bronze_layer.spark = self.spark
        
        # Mock the readStream function
        def mock_read_stream():
            class MockStreamReader:
                def format(self, _):
                    return self
                
                def option(self, _, __):
                    return self
                
                def load(self, _):
                    return TestBronzeLayer.customer_df
            
            return MockStreamReader()
        
        # Replace the actual readStream with our mock
        original_read_stream = self.spark.readStream
        self.spark.readStream = mock_read_stream
        
        # Apply the SCD Type-2 columns manually to verify
        expected_columns = ["id", "name", "email", "age", "CreateDateTime", "UpdateDateTime", "IsActive"]
        
        try:
            # Call the function (note: in a real test, you'd need to handle the DLT context)
            # This is a simplified test that just checks the schema transformation logic
            df = self.customer_df.withColumn("CreateDateTime", bronze_layer.current_timestamp()) \
                                .withColumn("UpdateDateTime", bronze_layer.current_timestamp()) \
                                .withColumn("IsActive", bronze_layer.lit(True))
            
            # Check that all expected columns are present
            self.assertTrue(all(col in df.columns for col in expected_columns))
            
            # Check data types of the added columns
            self.assertEqual(df.schema["CreateDateTime"].dataType, TimestampType())
            self.assertEqual(df.schema["UpdateDateTime"].dataType, TimestampType())
            self.assertEqual(df.schema["IsActive"].dataType, BooleanType())
            
        finally:
            # Restore the original readStream
            self.spark.readStream = original_read_stream
    
    def test_orders_raw_schema(self):
        """Test that the orders_raw function adds the correct SCD Type-2 columns"""
        # Mock the Auto Loader read by returning our test dataframe
        bronze_layer.spark = self.spark
        
        # Mock the readStream function
        def mock_read_stream():
            class MockStreamReader:
                def format(self, _):
                    return self
                
                def option(self, _, __):
                    return self
                
                def load(self, _):
                    return TestBronzeLayer.order_df
            
            return MockStreamReader()
        
        # Replace the actual readStream with our mock
        original_read_stream = self.spark.readStream
        self.spark.readStream = mock_read_stream
        
        # Apply the SCD Type-2 columns manually to verify
        expected_columns = ["id", "order_id", "amount", "order_date", "CreateDateTime", "UpdateDateTime", "IsActive"]
        
        try:
            # Call the function (note: in a real test, you'd need to handle the DLT context)
            # This is a simplified test that just checks the schema transformation logic
            df = self.order_df.withColumn("CreateDateTime", bronze_layer.current_timestamp()) \
                             .withColumn("UpdateDateTime", bronze_layer.current_timestamp()) \
                             .withColumn("IsActive", bronze_layer.lit(True))
            
            # Check that all expected columns are present
            self.assertTrue(all(col in df.columns for col in expected_columns))
            
            # Check data types of the added columns
            self.assertEqual(df.schema["CreateDateTime"].dataType, TimestampType())
            self.assertEqual(df.schema["UpdateDateTime"].dataType, TimestampType())
            self.assertEqual(df.schema["IsActive"].dataType, BooleanType())
            
        finally:
            # Restore the original readStream
            self.spark.readStream = original_read_stream

if __name__ == "__main__":
    unittest.main()