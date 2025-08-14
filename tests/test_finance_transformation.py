"""
Unit tests for the finance transformation module.
"""

import unittest
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as F
from src.finance_transformation import FinanceTransformation

class TestFinanceTransformation(unittest.TestCase):
    """Test cases for Finance Transformation module."""
    
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession and test data."""
        cls.spark = SparkSession.builder \
            .appName("Finance Transformation Test") \
            .master("local[2]") \
            .getOrCreate()
        
        # Create test data
        cls.create_test_data()
        
        # Set up configuration
        cls.config = {
            "faglflexa_path": "test_data/faglflexa",
            "bseg_path": "test_data/bseg",
            "golden_entity_path": "test_data/golden_entity",
            "golden_gl_path": "test_data/golden_gl",
            "golden_trading_partner_path": "test_data/golden_tp",
            "exchange_rate_path": "test_data/exchange_rate",
            "finance_table_path": "test_data/finance_table",
            "fiscal_year": 2023,
            "posting_period": 6,
            "realized_gain_account": "425000",
            "realized_loss_account": "525000",
            "unrealized_gain_account": "426000",
            "unrealized_loss_account": "526000"
        }
        
        # Initialize transformation class
        cls.finance_transform = FinanceTransformation(cls.spark, cls.config)
    
    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession."""
        cls.spark.stop()
    
    @classmethod
    def create_test_data(cls):
        """Create test data for unit tests."""
        # FAGLFLEXA test data
        faglflexa_data = [
            {"DOCNR": "1000000001", "RBUKRS": "1000", "RYEAR": 2023, "POPER": 6, "RACCT": "100000", "PRCTR": "PC001", "HSL": 1000.0, "KSL": 950.0, "RHCUR": "EUR", "RKCUR": "USD"},
            {"DOCNR": "1000000002", "RBUKRS": "1000", "RYEAR": 2023, "POPER": 6, "RACCT": "200000", "PRCTR": "PC002", "HSL": 2000.0, "KSL": 2000.0, "RHCUR": "USD", "RKCUR": "USD"},
            {"DOCNR": "1000000003", "RBUKRS": "2000", "RYEAR": 2023, "POPER": 6, "RACCT": "300000", "PRCTR": "PC003", "HSL": 3000.0, "KSL": 2800.0, "RHCUR": "GBP", "RKCUR": "EUR"},
            {"DOCNR": "1000000004", "RBUKRS": "3000", "RYEAR": 2023, "POPER": 7, "RACCT": "400000", "PRCTR": "PC004", "HSL": 4000.0, "KSL": 4000.0, "RHCUR": "USD", "RKCUR": "USD"}
        ]
        cls.faglflexa_df = cls.spark.createDataFrame(pd.DataFrame(faglflexa_data))
        cls.faglflexa_df.write.mode("overwrite").parquet("test_data/faglflexa")
        
        # BSEG test data
        bseg_data = [
            {"BELNR": "1000000001", "BUKRS": "1000", "GJAHR": 2023, "BLART": "AB"},
            {"BELNR": "1000000002", "BUKRS": "1000", "GJAHR": 2023, "BLART": "KZ"},
            {"BELNR": "1000000003", "BUKRS": "2000", "GJAHR": 2023, "BLART": "SA"},
            {"BELNR": "1000000004", "BUKRS": "3000", "GJAHR": 2023, "BLART": "DZ"}
        ]
        cls.bseg_df = cls.spark.createDataFrame(pd.DataFrame(bseg_data))
        cls.bseg_df.write.mode("overwrite").parquet("test_data/bseg")
        
        # Golden Entity test data
        golden_entity_data = [
            {"SourceCompanyCode": "1000", "GoldenEntityID": "LE001", "IsArchived": False},
            {"SourceCompanyCode": "2000", "GoldenEntityID": "LE002", "IsArchived": False},
            {"SourceCompanyCode": "3000", "GoldenEntityID": "LE003", "IsArchived": True}
        ]
        cls.golden_entity_df = cls.spark.createDataFrame(pd.DataFrame(golden_entity_data))
        cls.golden_entity_df.write.mode("overwrite").parquet("test_data/golden_entity")
        
        # Golden GL test data
        golden_gl_data = [
            {"SourceGLAccount": "100000", "GoldenGLID": "GL001"},
            {"SourceGLAccount": "200000", "GoldenGLID": "GL002"},
            {"SourceGLAccount": "300000", "GoldenGLID": "GL003"},
            {"SourceGLAccount": "400000", "GoldenGLID": "GL004"},
            {"SourceGLAccount": "425000", "GoldenGLID": "GL425"},
            {"SourceGLAccount": "525000", "GoldenGLID": "GL525"},
            {"SourceGLAccount": "426000", "GoldenGLID": "GL426"},
            {"SourceGLAccount": "526000", "GoldenGLID": "GL526"}
        ]
        cls.golden_gl_df = cls.spark.createDataFrame(pd.DataFrame(golden_gl_data))
        cls.golden_gl_df.write.mode("overwrite").parquet("test_data/golden_gl")
        
        # Golden Trading Partner test data
        golden_tp_data = [
            {"SourceTradingPartner": "PC001", "GoldenTradingPartnerID": "TP001"},
            {"SourceTradingPartner": "PC002", "GoldenTradingPartnerID": "TP002"},
            {"SourceTradingPartner": "PC003", "GoldenTradingPartnerID": "TP003"},
            {"SourceTradingPartner": "PC004", "GoldenTradingPartnerID": "TP004"}
        ]
        cls.golden_tp_df = cls.spark.createDataFrame(pd.DataFrame(golden_tp_data))
        cls.golden_tp_df.write.mode("overwrite").parquet("test_data/golden_tp")
        
        # Exchange Rate test data
        exchange_rate_data = [
            {"FromCurrency": "EUR", "ToCurrency": "USD", "FiscalYear": 2023, "Period": 6, "ExchangeRate": 1.1},
            {"FromCurrency": "GBP", "ToCurrency": "USD", "FiscalYear": 2023, "Period": 6, "ExchangeRate": 1.3},
            {"FromCurrency": "USD", "ToCurrency": "USD", "FiscalYear": 2023, "Period": 6, "ExchangeRate": 1.0},
            {"FromCurrency": "EUR", "ToCurrency": "USD", "FiscalYear": 2023, "Period": 7, "ExchangeRate": 1.12}
        ]
        cls.exchange_rate_df = cls.spark.createDataFrame(pd.DataFrame(exchange_rate_data))
        cls.exchange_rate_df.write.mode("overwrite").parquet("test_data/exchange_rate")
    
    def test_read_source_data(self):
        """Test reading source data."""
        faglflexa_df, bseg_df, golden_entity_df, golden_gl_df, golden_tp_df, exchange_rate_df = self.finance_transform.read_source_data()
        
        # Verify data was read correctly
        self.assertEqual(faglflexa_df.count(), 4)
        self.assertEqual(bseg_df.count(), 4)
        self.assertEqual(golden_entity_df.count(), 3)
        self.assertEqual(golden_gl_df.count(), 8)
        self.assertEqual(golden_tp_df.count(), 4)
        self.assertEqual(exchange_rate_df.count(), 4)
    
    def test_filter_by_fiscal_period(self):
        """Test filtering by fiscal period."""
        faglflexa_df = self.spark.read.parquet("test_data/faglflexa")
        filtered_df = self.finance_transform.filter_by_fiscal_period(faglflexa_df)
        
        # Verify filtering works correctly
        self.assertEqual(filtered_df.count(), 3)
        self.assertEqual(filtered_df.filter(F.col("POPER") != 6).count(), 0)
        self.assertEqual(filtered_df.filter(F.col("RYEAR") != 2023).count(), 0)
    
    def test_join_and_transform_data(self):
        """Test joining and transforming data."""
        # Read source data
        faglflexa_df = self.spark.read.parquet("test_data/faglflexa")
        bseg_df = self.spark.read.parquet("test_data/bseg")
        golden_entity_df = self.spark.read.parquet("test_data/golden_entity")
        golden_gl_df = self.spark.read.parquet("test_data/golden_gl")
        golden_tp_df = self.spark.read.parquet("test_data/golden_tp")
        exchange_rate_df = self.spark.read.parquet("test_data/exchange_rate")
        
        # Filter by fiscal period
        filtered_df = self.finance_transform.filter_by_fiscal_period(faglflexa_df)
        
        # Transform data
        transformed_df = self.finance_transform.join_and_transform_data(
            filtered_df, bseg_df, golden_entity_df, golden_gl_df, golden_tp_df, exchange_rate_df
        )
        
        # Verify transformation results
        self.assertEqual(transformed_df.count(), 2)  # One record is filtered out due to archived entity
        
        # Check columns exist
        expected_columns = [
            "FiscalYear", "PostingPeriod", "SourceFiscalYear", "SourcePeriod",
            "DocumentNumber", "CompCode", "LegalEntity", "GLAccount", "GoldenGLAcct",
            "TradingPartner", "GoldenTradingPartner", "LocalCurrency", "TransactionCurrency",
            "GainLossLC", "GainLossGC", "GainLossTC", "IsRealized", "OffsetAccount",
            "GoldenOffsetAccount", "ProcessedTimestamp"
        ]
        
        for col in expected_columns:
            self.assertIn(col, transformed_df.columns)
        
        # Verify specific transformations
        doc1 = transformed_df.filter(F.col("DocumentNumber") == "1000000001").collect()[0]
        self.assertEqual(doc1["FiscalYear"], 2023)
        self.assertEqual(doc1["PostingPeriod"], 6)
        self.assertEqual(doc1["LegalEntity"], "LE001")
        self.assertEqual(doc1["GoldenGLAcct"], "GL001")
        self.assertEqual(doc1["GoldenTradingPartner"], "TP001")
        self.assertEqual(doc1["GainLossLC"], 50.0)  # HSL - KSL = 1000 - 950 = 50
        self.assertEqual(doc1["IsRealized"], True)  # BLART = AB
    
    def test_write_finance_table(self):
        """Test writing to finance table."""
        # Create a simple test dataframe
        test_data = [
            {"FiscalYear": 2023, "PostingPeriod": 6, "DocumentNumber": "TEST001"}
        ]
        test_df = self.spark.createDataFrame(pd.DataFrame(test_data))
        
        # Write to finance table
        self.finance_transform.write_finance_table(test_df)
        
        # Verify data was written
        written_df = self.spark.read.parquet("test_data/finance_table")
        self.assertEqual(written_df.count(), 1)
        self.assertEqual(written_df.collect()[0]["DocumentNumber"], "TEST001")


if __name__ == "__main__":
    unittest.main()