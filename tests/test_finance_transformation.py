"""
Unit tests for the finance transformation module.
"""

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.finance_transformation import transform_finance_data, validate_data

class TestFinanceTransformation(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = SparkSession.builder \
            .appName("FinanceTransformationTest") \
            .master("local[2]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop Spark session after all tests."""
        cls.spark.stop()
    
    def create_mock_dataframes(self):
        """Create mock DataFrames for testing."""
        # FAGLFLEXA mock data
        faglflexa_data = [
            ("0L", "1000000001", "1000", "2023", "12", "X", "100000", "2000", "EUR", 1000.00, 1000.00, "EUR"),
            ("0L", "1000000002", "1000", "2023", "12", "X", "100001", "2000", "EUR", 2000.00, 2000.00, "EUR"),
            ("0L", "1000000003", "1000", "2023", "12", "X", "100002", "2000", "USD", 3000.00, 3000.00, "USD"),
            ("0L", "1000000004", "8000", "2023", "12", "X", "100003", "2000", "USD", 4000.00, 4000.00, "USD"),
            ("0L", "1000000005", "1000", "2023", "12", "", "100004", "2000", "EUR", 5000.00, 5000.00, "EUR")
        ]
        faglflexa_columns = ["RLDNR", "DOCNR", "RBUKRS", "RYEAR", "POPER", "XBILK", 
                            "RACCT", "RASSC", "RWCUR", "HSL", "TSL", "RHCUR"]
        faglflexa_df = self.spark.createDataFrame(faglflexa_data, faglflexa_columns)
        
        # BSEG mock data
        bseg_data = [
            ("1000000001", "1000", "2023", "200000", 1000.00, 1000.00, "1000000010"),
            ("1000000002", "1000", "2023", "200001", 2000.00, 2000.00, "1000000020"),
            ("1000000003", "1000", "2023", "200002", 3000.00, 3000.00, "1000000030"),
            ("1000000004", "8000", "2023", "200003", 4000.00, 4000.00, "1000000040"),
            ("1000000006", "1000", "2023", "200004", 6000.00, 6000.00, "1000000060")
        ]
        bseg_columns = ["BELNR", "BUKRS", "GJAHR", "HKONT", "DMBTR", "WRBTR", "AUGBL"]
        bseg_df = self.spark.createDataFrame(bseg_data, bseg_columns)
        
        # Entity Golden View mock data
        entity_data = [
            ("1000", "ENTITY1", "EUR"),
            ("2000", "ENTITY2", "USD"),
            ("3000", "ENTITY3", "GBP"),
            ("8000", "ENTITY8", "USD")
        ]
        entity_columns = ["CompanyCode", "GoldenEntity", "EntityCurrency"]
        entity_df = self.spark.createDataFrame(entity_data, entity_columns)
        
        # GL Golden View mock data
        gl_data = [
            ("100000", "GL1000", "Realized"),
            ("100001", "GL1001", "Unrealized"),
            ("100002", "GL1002", "Realized"),
            ("100003", "GL1003", "Other"),
            ("100004", "GL1004", "Other"),
            ("200000", "GL2000", "Other"),
            ("200001", "GL2001", "Other"),
            ("200002", "GL2002", "Other")
        ]
        gl_columns = ["SourceGLAccount", "GoldenGLAccount", "AccountType"]
        gl_df = self.spark.createDataFrame(gl_data, gl_columns)
        
        # Trading Partner Golden View mock data
        tp_data = [
            ("2000", "TP2000"),
            ("2001", "TP2001"),
            ("2002", "TP2002")
        ]
        tp_columns = ["SourceTradingPartner", "GoldenTradingPartner"]
        tp_df = self.spark.createDataFrame(tp_data, tp_columns)
        
        # BPC Exchange Rates mock data
        exchange_data = [
            ("EUR", "USD", "2023", "12", 1.1),
            ("GBP", "USD", "2023", "12", 1.3),
            ("USD", "USD", "2023", "12", 1.0)
        ]
        exchange_columns = ["FromCurrency", "ToCurrency", "FiscalYear", "Period", "ExchangeRate"]
        exchange_df = self.spark.createDataFrame(exchange_data, exchange_columns)
        
        return faglflexa_df, bseg_df, entity_df, gl_df, tp_df, exchange_df
    
    @patch('src.finance_transformation.load_source_data')
    def test_transform_finance_data(self, mock_load_source_data):
        """Test the finance data transformation logic."""
        # Set up mock data
        mock_dfs = self.create_mock_dataframes()
        mock_load_source_data.return_value = mock_dfs
        
        # Execute transformation
        result_df = transform_finance_data(self.spark, "2023", "12")
        
        # Assertions
        self.assertIsNotNone(result_df)
        
        # Convert to Pandas for easier assertions
        result_pd = result_df.toPandas()
        
        # Check record count - should be 2 records (realized/unrealized accounts)
        self.assertEqual(len(result_pd), 2)
        
        # Check specific transformations
        self.assertTrue("GoldenGLAcct" in result_pd.columns)
        self.assertTrue("GoldenTradingPartner" in result_pd.columns)
        self.assertTrue("GainLossGC" in result_pd.columns)
        
        # Check filtering logic - no company codes starting with 8
        self.assertTrue(not any(result_pd["CompCode"].str.startswith("8")))
        
        # Check fiscal year and posting period are set correctly
        self.assertTrue(all(result_pd["FiscalYear"] == "2023"))
        self.assertTrue(all(result_pd["PostingPeriod"] == "12"))
    
    def test_validate_data(self):
        """Test the data validation function."""
        # Create test data with some null values
        test_data = [
            ("2023", "12", "2023", "12", "1000000001", "1000", "ENTITY1", "100000", 
             "GL1000", "2000", "TP2000", 1100.00, 1000.00, "EUR", 1000.00, "EUR", 
             "200000", "GL2000", 1000.00, 1000.00, "1000000010", "ECC Everest"),
            ("2023", "12", "2023", "12", "1000000002", "1000", None, "100001", 
             "GL1001", "2000", "TP2000", 2200.00, 2000.00, "EUR", 2000.00, "EUR", 
             "200001", "GL2001", 2000.00, 2000.00, "1000000020", "ECC Everest")
        ]
        columns = [
            "FiscalYear", "PostingPeriod", "SourceFiscalYear", "SourcePeriod",
            "DocumentNumber", "CompCode", "LegalEntity", "GLAccount", 
            "GoldenGLAcct", "TradingPartner", "GoldenTradingPartner",
            "GainLossGC", "GainLossLC", "LocalCurrency", "GainLossTC",
            "TransactionCurrency", "OffsetAccount", "GoldenOffsetAccount",
            "OffsetAccountLCAmount", "OffsetAccountTCAmount",
            "OffsetClearingDocumentNumber", "SourceSystem"
        ]
        test_df =