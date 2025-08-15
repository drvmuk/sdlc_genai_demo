"""
Tests for the Finance Data Pipeline
"""

import unittest
from unittest.mock import patch, MagicMock
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd
from datetime import datetime

from src.finance_data_pipeline import FinanceDataPipeline

class TestFinanceDataPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession for all tests."""
        cls.spark = SparkSession.builder \
            .appName("Finance Data Pipeline Tests") \
            .master("local[2]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession after all tests."""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Create test data for FAGLFLEXA
        faglflexa_data = [
            ("0L", "2023", "1000", "DOC001", "1", "40000", "CC001", "PC001"),
            ("0L", "2023", "1000", "DOC002", "2", "50000", "CC002", "PC002"),
            ("0L", "2023", "2000", "DOC003", "3", "60000", "CC003", "PC003"),
            ("1L", "2023", "1000", "DOC004", "4", "70000", "CC004", "PC004")  # Should be filtered out
        ]
        faglflexa_schema = StructType([
            StructField("RLDNR", StringType(), True),
            StructField("RYEAR", StringType(), True),
            StructField("RBUKRS", StringType(), True),
            StructField("DOCNR", StringType(), True),
            StructField("POPER", StringType(), True),
            StructField("RACCT", StringType(), True),
            StructField("RCNTR", StringType(), True),
            StructField("PRCTR", StringType(), True)
        ])
        self.faglflexa_df = self.spark.createDataFrame(faglflexa_data, schema=faglflexa_schema)
        
        # Create test data for BSEG
        bseg_data = [
            ("DOC001", "1000", "2023", "X", "S", 1000.0, 1000.0, "USD", "1", "BP001", "ASG001", "Text 1"),
            ("DOC002", "1000", "2023", "X", "H", 2000.0, 2000.0, "EUR", "2", "BP002", "ASG002", "Text 2"),
            ("DOC003", "2000", "2023", "X", "S", 3000.0, 3000.0, "GBP", "3", "BP003", "ASG003", "Text 3"),
            ("DOC005", "3000", "2023", "", "S", 4000.0, 4000.0, "USD", "4", "BP004", "ASG004", "Text 4")  # No match or no XBILK
        ]
        bseg_schema = StructType([
            StructField("BELNR", StringType(), True),
            StructField("BUKRS", StringType(), True),
            StructField("GJAHR", StringType(), True),
            StructField("XBILK", StringType(), True),
            StructField("SHKZG", StringType(), True),
            StructField("DMBTR", DoubleType(), True),
            StructField("WRBTR", DoubleType(), True),
            StructField("WAERS", StringType(), True),
            StructField("BUZEI", StringType(), True),
            StructField("BUPLA", StringType(), True),
            StructField("ZUONR", StringType(), True),
            StructField("SGTXT", StringType(), True)
        ])
        self.bseg_df = self.spark.createDataFrame(bseg_data, schema=bseg_schema)
        
        # Create test data for golden views
        entity_data = [
            ("1000", "Company A", "EMEA"),
            ("2000", "Company B", "APAC"),
            ("3000", "Company C", "AMER")
        ]
        entity_schema = StructType([
            StructField("CompanyCode", StringType(), True),
            StructField("EntityName", StringType(), True),
            StructField("EntityRegion", StringType(), True)
        ])
        self.entity_df = self.spark.createDataFrame(entity_data, schema=entity_schema)
        
        gl_data = [
            ("40000", "Revenue", "P&L"),
            ("50000", "Cost of Goods Sold", "P&L"),
            ("60000", "Assets", "Balance Sheet")
        ]
        gl_schema = StructType([
            StructField("GLAccountNumber", StringType(), True),
            StructField("GLAccountName", StringType(), True),
            StructField("GLAccountType", StringType(), True)
        ])
        self.gl_df = self.spark.createDataFrame(gl_data, schema=gl_schema)
        
        tp_data = [
            ("1000", "Partner A"),
            ("2000", "Partner B"),
            ("3000", "Partner C")
        ]
        tp_schema = StructType([
            StructField("PartnerCompanyCode", StringType(), True),
            StructField("PartnerName", StringType(), True)
        ])
        self.tp_df = self.spark.createDataFrame(tp_data, schema=tp_schema)
        
        # Initialize pipeline with mock SparkSession
        self.pipeline = FinanceDataPipeline(self.spark)
    
    def test_extract_data(self):
        """Test data extraction with mocked data sources."""
        with patch.object(self.pipeline, 'spark') as mock_spark:
            # Setup mock returns
            mock_spark.read.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value.filter.return_value = self.faglflexa_df
            mock_spark.read.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value = self.bseg_df
            mock_spark.read.table.side_effect = [self.entity_df, self.gl_df, self.tp_df]
            
            # Call the method
            result = self.pipeline.extract_data()
            
            # Assertions
            self.assertEqual(len(result), 5)
            self.assertIn('faglflexa', result)
            self.assertIn('bseg', result)
            self.assertIn('entity', result)
            self.assertIn('gl_account', result)
            self.assertIn('trading_partner', result)
    
    def test_transform_data(self):
        """Test data transformation logic."""
        # Prepare input data
        data_sources = {
            'faglflexa': self.faglflexa_df,
            'bseg': self.bseg_df,
            'entity': self.entity_df,
            'gl_account': self.gl_df,
            'trading_partner': self.tp_df
        }
        
        # Call the method
        result_df = self.pipeline.transform_data(data_sources)
        
        # Convert to pandas for easier assertions
        result_pd = result_df.toPandas()
        
        # Assertions
        self.assertEqual(len(result_pd), 3)  # Only 3 rows should match after joining and filtering
        
        # Check transformed columns
        self.assertIn('FiscalYear', result_pd.columns)
        self.assertIn('PostingPeriod', result_pd.columns)
        self.assertIn('CompanyCode', result_pd.columns)
        self.assertIn('GLAccount', result_pd.columns)
        self.assertIn('Amount', result_pd.columns)
        self.assertIn('UniqueId', result_pd.columns)
        self.assertIn('LoadTimestamp', result_pd.columns)
        
        # Check specific transformations
        # Check that amounts are negated for H (Credit) entries
        credit_row = result_pd[result_pd['DocumentNumber'] == 'DOC002']
        self.assertTrue(len(credit_row) > 0)
        self.assertTrue(credit_row['Amount'].iloc[0] < 0)  # Should be negative for H
        
        # Check that debit entries remain positive
        debit_row = result_pd[result_pd['DocumentNumber'] == 'DOC001']
        self.assertTrue(len(debit_row) > 0)
        self.assertTrue(debit_row['Amount'].iloc[0] > 0)  # Should be positive for S
    
    def test_load_data(self):
        """Test data loading with mocked writer."""
        # Create a simple DataFrame to load
        test_data = [("2023", "01", "1000", "40000", 1000.0)]
        test_schema = StructType([
            StructField("FiscalYear", StringType(), True),
            StructField("PostingPeriod", StringType(), True),
            StructField("CompanyCode", StringType(), True),
            StructField("GLAccount", StringType(), True),
            StructField("Amount", DoubleType(), True)
        ])
        test_df = self.spark.createDataFrame(test_data, schema=test_schema)
        
        # Mock the DataFrame writer
        with patch.object(test_df, 'write') as mock_writer:
            mock_writer.format.return_value.mode.return_value.saveAsTable.return_value = None
            
            # Mock the count method
            test_df.count = MagicMock(return_value=1)
            
            # Call the method
            result = self.pipeline.load_data(test_df)
            
            # Assertions
            self.assertEqual(result, 1)  # Should return the count of rows loaded
            mock_writer.format.assert_called_with("delta")
            mock_writer.format.return_value.mode.assert_called_with("append")
            mock_writer.format.return_value.mode.return_value.saveAsTable.assert_called_with("target_database.Finance")
    
    def test_run_pipeline_success(self):
        """Test the full pipeline execution with mocked components."""
        # Mock the component methods
        self.pipeline.extract_data = MagicMock(return_value={
            'faglflexa': self.faglflexa_df,
            'bseg': self.bseg_df,
            'entity': self.entity_df,
            'gl_account': self.gl_df,
            'trading_partner': self.tp_df
        })
        
        # Create a simple transformed DataFrame
        test_data = [("2023", "01", "1000", "40000", 1000.0)]
        test_schema = StructType([
            StructField("FiscalYear", StringType(), True),
            StructField("PostingPeriod", StringType(), True),
            StructField("CompanyCode", StringType(), True),
            StructField("GLAccount", StringType(), True),
            StructField("Amount", DoubleType(), True)
        ])
        transformed_df = self.spark.createDataFrame(test_data, schema=test_schema)
        
        self.pipeline.transform_data = MagicMock(return_value=transformed_df)
        self.pipeline.load_data = MagicMock(return_value=1)
        
        # Call the method
        result = self.pipeline.run_pipeline()
        
        # Assertions
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["rows_processed"], 1)
        self.assertTrue("start_time" in result)
        self.assertTrue("end_time" in result)
        self.assertTrue("duration_seconds" in result)
        
        # Verify method calls
        self.pipeline.extract_data.assert_called_once()
        self.pipeline.transform_data.assert_called_once()
        self.pipeline.load_data.assert_called_once_with(transformed_df)
    
    def test_run_pipeline_failure(self):
        """Test pipeline execution with failure."""
        # Mock extract_data to raise an exception
        self.pipeline.extract_data = MagicMock(side_effect=Exception("Test error"))
        self.pipeline.send_email_notification = MagicMock()
        
        # Call the method
        result = self.pipeline.run_pipeline()
        
        # Assertions
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error"], "Test error")
        self.assertTrue("start_time" in result)
        self.assertTrue("end_time" in result)
        self.assertTrue("duration_seconds" in result)
        
        # Verify method calls
        self.pipeline.extract_data.assert_called_once()
        self.pipeline.send_email_notification.assert_called_once()

if __name__ == '__main__':
    unittest.main()