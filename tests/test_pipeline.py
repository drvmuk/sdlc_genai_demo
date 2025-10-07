import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
import datetime
from src.batch_pipeline import process_customer_order_data

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestCustomerOrderPipeline") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

@pytest.fixture(scope="module")
def sample_customer_data(spark):
    # Define schema
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    # Create sample data
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", "Jane Smith", "jane.smith@example.com", "South"),
        ("C003", "Bob Johnson", "bob.johnson@example.com", "East"),
        ("C004", "Alice Brown", "alice.brown@example.com", "West"),
        ("C005", None, "invalid@example.com", "North"),  # Null name
        ("C001", "John Doe", "john.doe@example.com", "North")  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def sample_order_data(spark):
    # Define schema
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    # Create sample data
    data = [
        ("O001", "Laptop", 1200.0, 1, "2023-01-15", "C001"),
        ("O002", "Mouse", 25.0, 2, "2023-01-20", "C002"),
        ("O003", "Keyboard", 50.0, 1, "2023-01-25", "C003"),
        ("O004", "Monitor", 300.0, 2, "2023-02-01", "C001"),
        ("O005", "Headphones", 100.0, 1, "2023-02-05", "C004"),
        ("O006", None, 200.0, 1, "2023-02-10", "C002"),  # Null item name
        ("O001", "Laptop", 1200.0, 1, "2023-01-15", "C001")  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)

def test_null_and_duplicate_removal(spark, sample_customer_data, sample_order_data, monkeypatch):
    # Mock the read operation to return our sample data
    def mock_read(*args, **kwargs):
        class MockReader:
            def option(self, *args, **kwargs):
                return self
                
            def load(self, path):
                if "customerdata" in path:
                    return sample_customer_data
                elif "orderdata" in path:
                    return sample_order_data
                return None
        return MockReader()
    
    # Mock the table existence check
    def mock_table_exists(*args, **kwargs):
        return False
    
    # Mock the write operation
    class MockDataFrameWriter:
        def __init__(self, df):
            self.df = df
            
        def format(self, *args, **kwargs):
            return self
            
        def mode(self, *args, **kwargs):
            return self
            
        def option(self, *args, **kwargs):
            return self
            
        def saveAsTable(self, *args, **kwargs):
            return self.df
    
    # Apply the mocks
    monkeypatch.setattr(spark, "read", mock_read)
    monkeypatch.setattr(spark._jsparkSession.catalog(), "tableExists", mock_table_exists)
    
    # Mock DataFrame.write
    original_write = spark.createDataFrame([], StructType([])).write
    monkeypatch.setattr(original_write.__class__, "format", lambda self, *args, **kwargs: MockDataFrameWriter(self._df))
    
    # Process the data
    results = process_customer_order_data(spark)
    
    # Verify customer data cleaning
    assert results["customer"].count() == 4  # Should remove duplicates and nulls
    
    # Verify order data cleaning
    assert results["order"].count() == 5  # Should remove duplicates and nulls
    
    # Verify TotalAmount calculation
    order_with_total = results["order"]
    for row in order_with_total.collect():
        assert row["TotalAmount"] == row["PricePerUnit"] * row["Qty"]

def test_customeraggregatespend_calculation(spark):
    # Create sample ordersummary data
    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("EmailId", StringType(), False),
        StructField("Region", StringType(), False),
        StructField("OrderId", StringType(), False),
        StructField("ItemName", StringType(), False),
        StructField("PricePerUnit", DoubleType(), False),
        StructField("Qty", IntegerType(), False),
        StructField("Date", StringType(), False),
        StructField("TotalAmount", DoubleType(), False),
        StructField("IsActive", BooleanType(), False),
        StructField("StartDate", TimestampType(), False),
        StructField("EndDate", TimestampType(), True)
    ])
    
    # Sample data with some inactive records
    now = datetime.datetime.now()
    data = [
        # Active records
        ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1000.0, 1, "2023-01-15", 1000.0, True, now, None),
        ("C001", "John Doe", "john@example.com", "North", "O002", "Mouse", 20.0, 2, "2023-01-15", 40.0, True, now, None),
        ("C002", "Jane Smith", "jane@example.com", "South", "O003", "Keyboard", 50.0, 1, "2023-01-20", 50.0, True, now, None),
        
        # Inactive records (should be excluded from aggregation)
        ("C001", "John Doe", "john@example.com", "East", "O001", "Laptop", 900.0, 1, "2023-01-15", 900.0, False, now, now),
    ]
    
    ordersummary_df = spark.createDataFrame(data, schema)
    
    # Calculate aggregates manually for verification
    expected_aggregates = {
        ("John Doe", "2023-01-15"): 1040.0,  # 1000 + 40
        ("Jane Smith", "2023-01-20"): 50.0
    }
    
    # Perform the aggregation
    aggregate_df = (ordersummary_df
                   .filter(F.col("IsActive") == True)
                   .groupBy("Name", "Date")
                   .agg(F.sum("TotalAmount").alias("TotalAmount")))
    
    # Verify the results
    for row in aggregate_df.collect():
        key = (row["Name"], row["Date"])
        assert key in expected_aggregates
        assert row["TotalAmount"] == expected_aggregates[key]