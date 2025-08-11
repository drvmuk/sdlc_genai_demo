"""
Tests for the data_loader module.
"""
import unittest
from unittest.mock import patch, MagicMock
import pytest
from pyspark.sql import SparkSession
from src.data_loader import DataLoader

class TestDataLoader(unittest.TestCase):
    """
    Test cases for the DataLoader class.
    """
    
    @classmethod
    def setUpClass(cls):
        """
        Set up the SparkSession for testing.
        """
        cls.spark = SparkSession.builder \
            .appName("TestDataLoader") \
            .master("local[1]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """
        Stop the SparkSession after testing.
        """
        cls.spark.stop()
    
    def setUp(self):
        """
        Set up the test environment.
        """
        self.data_loader = DataLoader(self.spark)
        
        # Create sample customer data
        self.customer_data = [
            (1, "John Doe", "123 Main St", "555-1234", "john@example.com"),
            (2, "Jane Smith", "456 Oak Ave", "555-5678", "jane@example.com")
        ]
        self.customer_schema = ["CustId", "Name", "Address", "Phone", "Email"]
        self.customer_df = self.spark.createDataFrame(self.customer_data, self.customer_schema)
        
        # Create sample order data
        self.order_data = [
            (101, 1, "2023-01-15", 100.0),
            (102, 2, "2023-01-16", 150.0)
        ]
        self.order_schema = ["OrderId", "CustId", "OrderDate", "Amount"]
        self.order_df = self.spark.createDataFrame(self.order_data, self.order_schema)
    
    @patch('src.data_loader.DeltaTable')
    def test_load_csv_to_delta(self, mock_delta_table):
        """
        Test loading CSV data to Delta table.
        """
        # Mock the spark read and write operations
        mock_read = MagicMock()
        mock_option = MagicMock()
        mock_csv = MagicMock()
        mock_write = MagicMock()
        mock_format = MagicMock()
        mock_mode = MagicMock()
        mock_save = MagicMock()
        
        self.data_loader.spark.read = mock_read
        mock_read.option.return_value = mock_option
        mock_option.option.return_value = mock_option
        mock_option.csv.return_value = self.customer_df
        
        self.customer_df.write = mock_write
        mock_write.format.return_value = mock_format
        mock_format.mode.return_value = mock_mode
        mock_mode.saveAsTable = mock_save
        
        # Call the method
        result = self.data_loader.load_csv_to_delta(
            "test_source_path",
            "test_target_table"
        )
        
        # Assertions
        mock_read.option.assert_called_with("inferSchema", "true")
        mock_option.option.assert_called_with("header", "true")
        mock_option.csv.assert_called_with("test_source_path")
        mock_write.format.assert_called_with("delta")
        mock_format.mode.assert_called_with("overwrite")
        mock_mode.saveAsTable.assert_called_with("test_target_table")
        
        # Check that result has the expected columns
        expected_columns = self.customer_schema + ["effective_date", "is_current", "end_date"]
        self.assertTrue(all(col in result.columns for col in expected_columns))
    
    @patch('src.data_loader.DeltaTable')
    def test_generate_order_summary(self, mock_delta_table):
        """
        Test generating order summary.
        """
        # Mock the spark table method
        self.data_loader.spark.table = MagicMock()
        self.data_loader.spark.table.side_effect = lambda x: self.customer_df if x == self.data_loader.customer_target else self.order_df
        
        # Mock the write operations
        mock_write = MagicMock()
        mock_format = MagicMock()
        mock_mode = MagicMock()
        mock_save = MagicMock()
        
        # Setup the mock chain
        self.order_df.join = MagicMock(return_value=self.order_df)
        self.order_df.select = MagicMock(return_value=self.order_df)
        self.order_df.write = mock_write
        mock_write.format.return_value = mock_format
        mock_format.mode.return_value = mock_mode
        mock_mode.saveAsTable = mock_save
        
        # Call the method
        result = self.data_loader.generate_order_summary()
        
        # Assertions
        self.data_loader.spark.table.assert_any_call(self.data_loader.customer_target)
        self.data_loader.spark.table.assert_any_call(self.data_loader.order_target)
        self.order_df.join.assert_called_once()
        self.order_df.select.assert_called_once()
        mock_write.format.assert_called_with("delta")
        mock_format.mode.assert_called_with("overwrite")
        mock_mode.saveAsTable.assert_called_with(self.data_loader.order_summary_target)
    
    @patch('src.data_loader.DeltaTable')
    def test_update_order_summary_for_customer_changes(self, mock_delta_table):
        """
        Test updating order summary for customer changes.
        """
        # Mock the spark table method
        self.data_loader.spark.table = MagicMock()
        self.data_loader.spark.table.side_effect = lambda x: self.customer_df if x == self.data_loader.customer_target else self.order_df
        
        # Mock the DeltaTable.forName method
        mock_delta_instance = MagicMock()
        mock_delta_table.forName.return_value = mock_delta_instance
        
        # Mock the merge operations
        mock_alias = MagicMock()
        mock_merge = MagicMock()
        mock_when_matched = MagicMock()
        mock_when_not_matched = MagicMock()
        mock_execute = MagicMock()
        
        # Setup the mock chain
        mock_delta_instance.alias.return_value = mock_alias
        mock_alias.merge.return_value = mock_merge
        mock_merge.whenMatchedUpdateAll.return_value = mock_when_matched
        mock_when_matched.whenNotMatchedInsertAll.return_value = mock_when_not_matched
        mock_when_not_matched.execute.return_value = None
        
        # Mock the join and select operations
        self.order_df.join = MagicMock(return_value=self.order_df)
        self.order_df.select = MagicMock(return_value=self.order_df)
        
        # Call the method
        result = self.data_loader.update_order_summary_for_customer_changes()
        
        # Assertions
        self.data_loader.spark.table.assert_any_call(self.data_loader.customer_target)
        self.data_loader.spark.table.assert_any_call(self.data_loader.order_target)
        mock_delta_table.forName.assert_called_with(self.data_loader.spark, self.data_loader.order_summary_target)
        mock_delta_instance.alias.assert_called_with("target")
        mock_alias.merge.assert_called_once()
        mock_merge.whenMatchedUpdateAll.assert_called_once()
        mock_when_matched.whenNotMatchedInsertAll.assert_called_once()
        mock_when_not_matched.execute.assert_called_once()
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()