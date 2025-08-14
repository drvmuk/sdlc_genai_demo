"""
Tests for the Finance Data Processor.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
import os
import tempfile
import shutil
from decimal import Decimal

from src.finance_data_processor import FinanceDataProcessor

class TestFinanceDataProcessor(unittest.TestCase):
    """
    Test cases for the Finance Data Processor.
    """
    
    @classmethod
    def setUpClass(cls):
        """
        Set up SparkSession and test data before running tests.
        """
        # Create a SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("TestFinanceDataProcessor") \
            .master("local[2]") \
            .getOrCreate()
            
        # Create temporary directory for test data
        cls.test_dir = tempfile.mkdtemp()
        
        # Create test data
        cls._create_test_data()
    
    @classmethod
    def tearDownClass(cls):
        """
        Clean up resources after tests.
        """
        # Stop SparkSession
        cls.spark.stop()
        
        # Remove temporary directory
        shutil.rmtree(cls.test_dir)
    
    @classmethod
    def _create_test_data(cls):
        """
        Create test data for the tests.
        """
        # FAGLFLEXA test data
        faglflexa_schema = StructType([
            StructField("DOCNR", StringType(), False),
            StructField("RBUKRS", StringType(), False),
            StructField("RYEAR", StringType(), False),
            StructField("RLDNR", StringType(), False),
            StructField("XBILK", StringType(), False),
            StructField("RACCT", StringType(), False),
            StructField("RASSC", StringType(), True),
            StructField("POPER", StringType(), False),
            StructField("BLDAT", DateType(), False),
            StructField("BUDAT", DateType(), False),
            StructField("RWCUR", StringType(), False),
            StructField("HSL", DecimalType(15, 2), False),
            StructField("KSL", DecimalType(15, 2), False)
        ])
        
        faglflexa_data = [
            ("1000000001", "1000", "2023", "0L", "X", "100000", "TP001", "01", 
             "2023-01-15", "2023-01-15", "USD", Decimal("1000.00"), Decimal("900.00")),
            ("1000000002", "2000", "2023", "0L", "X", "200000", "TP002", "01", 
             "2023-01-20", "2023-01-20", "EUR", Decimal("2000.00"), Decimal("1800.00")),
            ("1000000003", "3000", "2023", "0L", "X", "300000", None, "01", 
             "2023-01-25", "2023-01-25", "GBP", Decimal("3000.00"), Decimal("2700.00")),
            ("1000000004", "8000", "2023", "0L", "X", "400000", "TP004", "01", 
             "2023-01-30", "2023-01-30", "JPY", Decimal("4000.00"), Decimal("3600.00"))
        ]
        
        cls.faglflexa_df = cls.spark.createDataFrame(faglflexa_data, faglflexa_schema)
        cls.faglflexa_path = os.path.join(cls.test_dir, "faglflexa")
        cls.faglflexa_df.write.parquet(cls.faglflexa_path)
        
        # BSEG test data
        bseg_schema = StructType([
            StructField("DOCNR", StringType(), False),
            StructField("RBUKRS", StringType(), False),
            StructField("RYEAR", StringType(), False),
            StructField("BELNR", StringType(), False)
        ])
        
        bseg_data = [
            ("1000000001", "1000", "2023", "100001"),
            ("1000000002", "2000", "2023", "200002"),
            ("1000000003", "3000", "2023", "300003"),
            ("1000000004", "8000", "2023", "400004")
        ]
        
        cls.bseg_df = cls.spark.createDataFrame(bseg_data, bseg_schema)
        cls.bseg_path = os.path.join(cls.test_dir, "bseg")
        cls.bseg_df.write.parquet(cls.bseg_path)
        
        # Entity golden view test data
        entity_schema = StructType([
            StructField("source_company_code", StringType(), False),
            StructField("golden_entity_id", StringType(), False)
        ])
        
        entity_data = [
            ("1000", "ENTITY_1000"),
            ("2000", "ENTITY_2000"),
            ("3000", "ENTITY_3000"),
            ("8000", "ENTITY_8000")
        ]
        
        cls.entity_df = cls.spark.createDataFrame(entity_data, entity_schema)
        cls.entity_path = os.path.join(cls.test_dir, "entity_golden_view")
        cls.entity_df.write.parquet(cls.entity_path)
        
        # GL golden view test data
        gl_schema = StructType([
            StructField("source_gl_account", StringType(), False),
            StructField("golden_gl_account_id", StringType(), False)
        ])
        
        gl_data = [
            ("100000", "GL_100000"),
            ("200000", "GL_200000"),
            ("300000", "GL_300000"),
            ("400000", "GL_400000"),
            ("FX_GAIN_ACCOUNT", "GL_FX_GAIN"),
            ("FX_LOSS_ACCOUNT", "GL_FX_LOSS")
        ]
        
        cls.gl_df = cls.spark.createDataFrame(gl_data, gl_schema)
        cls.gl_path = os.path.join(cls.test_dir, "gl_golden_view")
        cls.gl_df.write.parquet(cls.gl_path)
        
        # Trading Partner golden view test data
        tp_schema = StructType([
            StructField("source_trading_partner", StringType(), False),
            StructField("golden_trading_partner_id", StringType(), False)
        ])
        
        tp_data = [
            ("TP001", "GTP_001"),
            ("TP002", "GTP_002"),
            ("TP003", "GTP_003"),
            ("TP004", "GTP_004")
        ]
        
        cls.tp_df = cls.spark.createDataFrame(tp_data, tp_schema)
        cls.tp_path = os.path.join(cls.test_dir, "trading_partner_golden_view")
        cls.tp_df.write.parquet(cls.tp_path)
        
        # Exchange rates test data
        exchange_schema = StructType([
            StructField("company_code", StringType(), False),
            StructField("rate_date", DateType(), False),
            StructField("local_currency", StringType(), False),
            StructField("standard_rate", DecimalType(15, 5), False),
            StructField("special_rate", DecimalType(15, 5), False)
        ])
        
        exchange_data = [
            ("1000", "2023-01-15", "USD", Decimal("1.10000"), Decimal("1.12000")),
            ("2000", "2023-01-20", "EUR", Decimal("1.20000"), Decimal("1.22000")),
            ("3000", "2023-01-25", "GBP", Decimal("1.30000"), Decimal("1.32000")),
            ("4000", "2023-01-30", "JPY", Decimal("0.00900"), Decimal("0.00910"))
        ]
        
        cls.exchange_df = cls.spark.createDataFrame(exchange_data, exchange_schema)
        cls.exchange_path = os.path.join(cls.test_dir, "exchange_rates")
        cls.exchange_df.write.parquet(cls.exchange_path)
        
        # Output path
        cls.output_path = os.path.join(cls.test_dir, "output")
    
    def test_read_source_data(self):
        """
        Test reading source data.
        """
        # Create config
        config = {
            "faglflexa_path": self.faglflexa_path,
            "bseg_path": self.bseg_path,
            "entity_golden_view_path": self.entity_path,
            "gl_golden_view_path": self.gl_path,
            "trading_partner_golden_view_path": self.tp_path,
            "exchange_rates_path": self.exchange_path,
            "output_path": self.output_path
        }
        
        # Create processor
        processor = FinanceDataProcessor(self.spark, config)
        
        # Read source data
        source_data = processor.read_source_data()
        
        # Verify all sources were read
        self.assertIn("faglflexa", source_data)
        self.assertIn("bseg", source_data)
        self.assertIn("entity_golden_view", source_data)
        self.assertIn("gl_golden_view", source_data)
        self.assertIn("trading_partner_golden_view", source_data)
        self.assertIn("exchange_rates", source_data)
        
        # Verify counts
        self.assertEqual(source_data["faglflexa"].count(), 4)
        self.assertEqual(source_data["bseg"].count(), 4)
        self.assertEqual(source_data["entity_golden_view"].count(), 4)
        self.assertEqual(source_data["gl_golden_view"].count(), 6)
        self.assertEqual(source_data["trading_partner_golden_view"].count(), 4)
        self.assertEqual(source_data["exchange_rates"].count(), 4)
    
    def test_process_data(self):
        """
        Test processing data.
        """
        # Create config
        config = {
            "faglflexa_path": self.faglflexa_path,
            "bseg_path": self.bseg_path,
            "entity_golden_view_path": self.entity_path,
            "gl_golden_view_path": self.gl_path,
            "trading_partner_golden_view_path": self.tp_path,
            "exchange_rates_path": self.exchange_path,
            "output_path": self.output_path
        }
        
        # Create processor
        processor = FinanceDataProcessor(self.spark, config)
        
        # Read source data
        source_data = processor.read_source_data()
        
        # Process data
        result_df = processor.process_data(source_data)
        
        # Verify result
        self.assertIsNotNone(result_df)
        
        # Verify filtering (should exclude RBUKRS starting with '8')
        self.assertEqual(result_df.count(), 3)
        
        # Verify columns
        expected_columns = [
            "DocumentNumber", "FiscalYear", "Period", "PostingDate", "DocumentDate",
            "GoldenEntityId", "GoldenGLAccountId", "GoldenTradingPartnerId",
            "TransactionCurrency", "AmountInLocalCurrency", "AmountInGroupCurrency",
            "GainLossLC", "GainLossGC", "OffsetGoldenGLAccountId"
        ]
        
        actual_columns = result_df.columns
        for col in expected_columns:
            self.assertIn(col, actual_columns)
    
    def test_end_to_end_processing(self):
        """
        Test end-to-end processing.
        """
        # Create config
        config = {
            "faglflexa_path": self.faglflexa_path,
            "bseg_path": self.bseg_path,
            "entity_golden_view_path": self.entity_path,
            "gl_golden_view_path": self.gl_path,
            "trading_partner_golden_view_path": self.tp_path,
            "exchange_rates_path": self.exchange_path,
            "output_path": self.output_path
        }
        
        # Create processor
        processor = FinanceDataProcessor(self.spark, config)
        
        # Run processor
        processor.run()
        
        # Verify output was written
        output_exists = os.path.exists(self.output_path)
        self.assertTrue(output_exists)
        
        # Read output
        output_df = self.spark.read.parquet(self.output_path)
        
        # Verify output
        self.assertIsNotNone(output_df)
        self.assertEqual(output_df.count(), 3)  # Excluding records with RBUKRS starting with '8'
        
        # Verify partitioning
        partitions = [f for f in os.listdir(self.output_path) if f.startswith("FiscalYear=")]
        self.assertTrue(len(partitions) > 0)


if __name__ == "__main__":
    unittest.main()