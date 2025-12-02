import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime
import os
import tempfile

class TestDeltaLiveTables(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDeltaLiveTables") \
            .master("local[1]") \
            .getOrCreate()
        
        # Create test data
        # Customer data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),
            ("C003", "Bob Johnson", "bob@example.com", "East"),  # Duplicate
            ("C004", None, "alice@example.com", "West"),  # Contains null
            ("C005", "Charlie Brown", "charlie@example.com", "North")
        ]
        
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        cls.customer_df = cls.spark.createDataFrame(customer_data, schema=customer_schema)
        
        # Order data
        order_data = [
            ("O001", "Laptop", 1000.0, 2, datetime.date(2023, 1, 15), "C001"),
            ("O002", "Phone", 500.0, 1, datetime.date(2023, 2, 20), "C002"),
            ("O003", "Tablet", 300.0, 3, datetime.date(2023, 3, 10), "C003"),
            ("O003", "Tablet", 300.0, 3, datetime.date(2023, 3, 10), "C003"),  # Duplicate
            ("O004", "Monitor", None, 2, datetime.date(2023, 4, 5), "C001"),  # Contains null
            ("O005", "Keyboard", 50.0, 5, datetime.date(2023, 5, 12), "C005")
        ]
        
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", DateType(), True),
            StructField("CustId", StringType(), True)
        ])
        
        cls.order_df = cls.spark.createDataFrame(order_data, schema=order_schema)
        
        # Create temporary directory for test data
        cls.temp_dir = tempfile.mkdtemp()
        cls.customer_path = os.path.join(cls.temp_dir, "customerdata")
        cls.order_path = os.path.join(cls.temp_dir, "orderdata")
        
        # Write test data to temporary directory
        cls.customer_df.write.format("csv").option("header", "true").save(cls.customer_path)
        cls.order_df.write.format("csv").option("header", "true").save(cls.order_path)
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
        
        # Clean up temporary directory
        import shutil
        shutil.rmtree(cls.temp_dir)
    
    def test_dlt_transformations(self):
        """
        Test the transformations that would be applied in Delta Live Tables.
        Since we can't directly run DLT in a test, we simulate the transformations.
        """
        # Simulate customer_silver transformation
        customer_silver = self.customer_df \
            .dropDuplicates(["CustId"]) \
            .filter(self.customer_df.CustId.isNotNull() & 
                    self.customer_df.Name.isNotNull() & 
                    self.customer_df.EmailId.isNotNull() & 
                    self.customer_df.Region.isNotNull())
        
        # Verify customer_silver
        self.assertEqual(customer_silver.count(), 4)  # Should have removed duplicates and nulls
        
        # Simulate order_silver transformation
        from pyspark.sql.functions import col
        order_silver = self.order_df \
            .withColumn("TotalAmount", col("PricePerUnit") * col("Qty")) \
            .dropDuplicates(["OrderId"]) \
            .filter(self.order_df.OrderId.isNotNull() & 
                    self.order_df.ItemName.isNotNull() & 
                    self.order_df.PricePerUnit.isNotNull() & 
                    self.order_df.Qty.isNotNull() & 
                    self.order_df.Date.isNotNull() & 
                    self.order_df.CustId.isNotNull())
        
        # Verify order_silver
        self.assertEqual(order_silver.count(), 4)  # Should have removed duplicates and nulls
        
        # Check TotalAmount calculation
        order_with_total = order_silver.filter(order_silver.OrderId == "O001").collect()[0]
        self.assertEqual(order_with_total.TotalAmount, 2000.0)  # 1000.0 * 2
        
        # Simulate join for ordersummary
        from pyspark.sql.functions import lit
        joined_df = customer_silver.join(
            order_silver,
            on="CustId",
            how="inner"
        ).select(
            customer_silver["CustId"],
            customer_silver["Name"],
            customer_silver["EmailId"],
            customer_silver["Region"],
            order_silver["OrderId"],
            order_silver["ItemName"],
            order_silver["PricePerUnit"],
            order_silver["Qty"],
            order_silver["Date"],
            order_silver["TotalAmount"]
        )
        
        # Verify joined data
        self.assertEqual(joined_df.count(), 4)
        
        # Simulate customeraggregatespend
        from pyspark.sql.functions import sum as sum_
        aggregate_df = joined_df.groupBy("Name", "Date") \
            .agg(sum_("TotalAmount").alias("TotalAmount"))
        
        # Verify aggregation
        self.assertEqual(aggregate_df.count(), 4)  # Each customer has one order on a different date

if __name__ == "__main__":
    unittest.main()