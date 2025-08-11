"""
Tests for the main module.
"""
import unittest
from unittest.mock import patch, MagicMock
import pytest
from src.main import main

class TestMain(unittest.TestCase):
    """
    Test cases for the main module.
    """
    
    @patch('src.main.get_spark_session')
    @patch('src.main.DataLoader')
    @patch('src.main.argparse.ArgumentParser.parse_args')
    def test_main_load_data(self, mock_parse_args, mock_data_loader_class, mock_get_spark_session):
        """
        Test the main function with the load_data job.
        """
        # Mock the arguments
        mock_args = MagicMock()
        mock_args.job = 'load_data'
        mock_parse_args.return_value = mock_args
        
        # Mock the SparkSession
        mock_spark = MagicMock()
        mock_get_spark_session.return_value = mock_spark
        
        # Mock the DataLoader
        mock_data_loader = MagicMock()
        mock_data_loader_class.return_value = mock_data_loader
        
        # Call the main function
        main()
        
        # Assertions
        mock_get_spark_session.assert_called_once()
        mock_data_loader_class.assert_called_once_with(mock_spark)
        mock_data_loader.load_csv_to_delta.assert_any_call(
            mock_data_loader.customer_source_path,
            mock_data_loader.customer_target
        )
        mock_data_loader.load_csv_to_delta.assert_any_call(
            mock_data_loader.order_source_path,
            mock_data_loader.order_target
        )
        mock_data_loader.generate_order_summary.assert_called_once()
    
    @patch('src.main.get_spark_session')
    @patch('src.main.DataLoader')
    @patch('src.main.argparse.ArgumentParser.parse_args')
    def test_main_update_summary(self, mock_parse_args, mock_data_loader_class, mock_get_spark_session):
        """
        Test the main function with the update_summary job.
        """
        # Mock the arguments
        mock_args = MagicMock()
        mock_args.job = 'update_summary'
        mock_parse_args.return_value = mock_args
        
        # Mock the SparkSession
        mock_spark = MagicMock()
        mock_get_spark_session.return_value = mock_spark
        
        # Mock the DataLoader
        mock_data_loader = MagicMock()
        mock_data_loader_class.return_value = mock_data_loader
        
        # Call the main function
        main()
        
        # Assertions
        mock_get_spark_session.assert_called_once()
        mock_data_loader_class.assert_called_once_with(mock_spark)
        mock_data_loader.update_order_summary_for_customer_changes.assert_called_once()
    
    @patch('src.main.get_spark_session')
    @patch('src.main.DataLoader')
    @patch('src.main.argparse.ArgumentParser.parse_args')
    @patch('src.main.logger.error')
    def test_main_exception(self, mock_logger_error, mock_parse_args, mock_data_loader_class, mock_get_spark_session):
        """
        Test the main function when an exception occurs.
        """
        # Mock the arguments
        mock_args = MagicMock()
        mock_args.job = 'load_data'
        mock_parse_args.return_value = mock_args
        
        # Mock the SparkSession to raise an exception
        mock_get_spark_session.side_effect = Exception("Test exception")
        
        # Call the main function and expect an exception
        with self.assertRaises(Exception):
            main()
        
        # Assertions
        mock_logger_error.assert_called_once()

if __name__ == '__main__':
    unittest.main()