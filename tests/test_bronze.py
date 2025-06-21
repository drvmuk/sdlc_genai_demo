import unittest
from pyspark.sql import SparkSession
from src.bronze.load_customer_raw import load_customer_raw

class TestBronzeLayer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test").getOrCreate()
        
    def test_load_customer_raw(self):
        # Create a dummy DataFrame
        data = [("1", "John"), ("2", "Jane")]
        df = self.spark.createDataFrame(data, ["id", "name"])
        
        # Apply the transformation
        result_df = load_customer_raw(df)
        
        # Assertions
        self.assertEqual(result_df.count(), 2)
        self.assertTrue("CreateDateTime" in result_df.columns)
        
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()