import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.module1 import main

class TestModule1(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_main(self):
        # Create a sample DataFrame
        data = [(1, "a"), (2, "b"), (3, "c")]
        df = self.spark.createDataFrame(data, ["id", "value"])

        # Write the DataFrame to a temporary CSV file
        df.write.csv("data/sample.csv", header=True)

        # Run the main function
        main()

        # Read the output CSV file and verify its contents
        output_df = self.spark.read.csv("output.csv", header=True, inferSchema=True)
        self.assertEqual(output_df.count(), 3)
        self.assertTrue("double_value" in output_df.columns)

if __name__ == "__main__":
    unittest.main()