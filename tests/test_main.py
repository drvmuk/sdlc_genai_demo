"""
Unit tests for main module
"""

import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
import src.main as main
from src.etl_processor import ETLProcessor

class TestMain(unittest.TestCase):
    
    @patch('src.main.get_spark_session')
    @patch('src.main.ETLProcessor')
    def test_main_function(self, mock_etl_processor_class, mock_get_spark_session):
        """
        Test that the main function initializes and runs the ETL processor
        """
        # Setup mocks
        mock_spark = MagicMock(spec=SparkSession)
        mock_get_spark_session.return_value = mock_spark
        
        mock_etl_processor = MagicMock(spec=ETLProcessor)
        mock_etl_processor_class.return_value = mock_etl_processor
        
        # Call the main function
        main.main()
        
        # Verify the expected calls
        mock_get_spark_session.assert_called_once()
        mock_etl_processor_class.assert_called_once_with(mock_spark)
        mock_etl_processor.run_etl_pipeline.assert_called_once()

if __name__ == "__main__":
    unittest.main()