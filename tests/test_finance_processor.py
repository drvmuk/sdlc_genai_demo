"""
Unit tests for Finance Data Processor
"""
import unittest
from unittest.mock import patch, MagicMock

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from src.finance_processor import (
    create_spark_session,
    read_source_data,
    transform_data,
    write_target_data
)


class TestFinanceProcessor(unittest.TestCase):
    """Test cases for Finance Data Processor"""

    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        cls.spark = SparkSession.builder \
            .appName("FinanceProcessorTest") \
            .master("local[*]") \
            .getOrCreate()
        
        # Define sample schemas
        cls.faglflexa_schema = StructType([
            StructField("RLDNR", StringType(), False),
            StructField("RBUKRS", StringType(), False),
            StructField("GJAHR", StringType(), False),
            StructField("POPER", StringType(), False),
            StructField("DOCNR", StringType(), False),
            StructField("RYEAR", StringType(), False),
            StructField("RACCT", StringType(), False),
            StructField("RHCUR", StringType(), False),
            StructField("RKCUR", StringType(), False),
            StructField("HSL", DoubleType(), False),
            StructField("KSL", DoubleType(), False)
        ])
        
        cls.bseg_schema = StructType([
            StructField("BELNR", StringType(), False),
            StructField("BUKRS", StringType(), False),
            StructField("GJAHR", StringType(), False),
            StructField("BUDAT", StringType(), False),
            StructField("BLART", StringType(), False),
            StructField("KUNNR", StringType(), True)
        ])
        
        cls.golden_entity_schema = StructType([
            StructField("EntityCode", StringType(), False),
            StructField("EntityName", StringType(), False),
            StructField("EntityRegion", StringType(), False)
        ])
        
        cls.golden_gl_schema = StructType([
            StructField("GLCode", StringType(), False),
            StructField("GLName", StringType(), False),
            StructField("GLCategory", StringType(), False)
        ])
        
        cls.golden_tp_schema = StructType([
            StructField("PartnerCode", StringType(), False),
            StructField("PartnerName", StringType(), False),
            StructField("PartnerType", StringType(), False)
        ])
        
        cls.bpc_rates_schema = StructType([
            StructField("FiscalYear", StringType(), False),
            StructField("Period", StringType(), False),
            StructField("FromCurrency", StringType(), False),
            StructField("ToCurrency", StringType(), False),
            StructField("Rate", DoubleType(), False)
        ])
        
        # Create sample data
        faglflexa_data = [
            ("0L", "1000", "2023", "001", "1000000001", "2023", "400000", "USD", "USD", 1000.0, 1000.0),
            ("0L", "1000", "2023", "001", "1000000002", "2023", "400001", "EUR", "USD", 2000.0, 2200.0),
            ("0L", "2000", "2023", "002", "2000000001", "2023", "500000", "GBP", "EUR", 3000.0, 3500.0),
            ("0X", "3000", "2023", "003", "3000000001", "2023", "600000", "JPY", "JPY", 4000.0, 4000.0)  # Should be filtered out
        ]
        
        bseg_data = [
            ("1000000001", "1000", "2023", "20230115", "KR", "C1000"),
            ("1000000002", "1000", "2023", "20230120", "KR", "C2000"),
            ("2000000001", "2000", "2023", "20230210", "KZ", "C3000"),
            ("3000000001", "3000", "2023", "20230305", "KG", None)
        ]
        
        golden_entity_data = [
            ("1000", "Entity A", "EMEA"),
            ("2000", "Entity B", "APAC"),
            ("3000", "Entity C", "AMER")
        ]
        
        golden_gl_data = [
            ("400000", "Revenue", "Income"),
            ("400001", "Sales", "Income"),
            ("500000", "Expense", "Expense"),
            ("600000", "Asset", "Asset")
        ]
        
        golden_tp_data = [
            ("C1000", "Customer 1", "Customer"),
            ("C2000", "Customer 2", "Customer"),
            ("C3000", "Customer 3", "Customer")
        ]
        
        bpc_rates_data = [
            ("2023", "001", "USD", "USD", 1.0),
            ("2023", "001", "EUR", "USD", 1.1),
            ("2023", "002", "GBP", "USD", 1.25),
            ("2023", "003", "JPY", "USD", 0.0075)
        ]
        
        # Create DataFrames
        cls.faglflexa_df = cls.spark.createDataFrame(faglflexa_data, cls.faglflexa_schema)
        cls.bseg_df = cls.spark.createDataFrame(bseg_data, cls.bseg_schema)
        cls.golden_entity_df = cls.spark.createDataFrame(golden_entity_data, cls.golden_entity_schema)
        cls.golden_gl_df = cls.spark.createDataFrame(golden_gl_data, cls.golden_gl_schema)
        cls.golden_tp_df = cls.spark.createDataFrame(golden_tp_data, cls.golden_tp_schema)
        cls.bpc_rates_df = cls.spark.createDataFrame(bpc_rates_data, cls.bpc_rates_schema)
        
        # Create data dictionary
        cls.data_dict = {
            'FAGLFLEXA': cls.faglflexa_df,
            'BSEG': cls.bseg_df,
            'GOLDEN_ENTITY': cls.golden_entity_df,
            'GOLDEN_GL': cls.golden_gl_df,
            'GOLDEN_TRADING_PARTNER': cls.golden_tp_df,
            'BPC_EXCHANGE_RATES': cls.bpc_rates_df
        }

    @classmethod
    def tearDownClass(cls):
        """Tear down test environment"""
        cls.spark.stop()

    def test_create_spark_session(self):
        """Test create_spark_session function"""
        with patch('src.finance_processor.SparkSession') as mock_spark:
            mock_builder = MagicMock()
            mock_spark.builder = mock_builder
            mock_builder.appName.return_value = mock_builder
            mock_builder.config.return_value = mock_builder
            mock_builder.getOrCreate.return_value = "mock_session"
            
            result = create_spark_session()
            
            mock_builder.appName.assert_called_with("Finance Data Processor")
            mock_builder.getOrCreate.assert_called_once()
            self.assertEqual(result, "mock_session")

    def test_read_source_data(self):
        """Test read_source_data function"""
        with patch('src.finance_processor.spark.read') as mock_read, \
             patch('src.finance_processor.spark.table') as mock_table:
            
            mock_read.parquet.side_effect = [self.faglflexa_df, self.bseg_df]
            mock_table.side_effect = [
                self.golden_entity_df, 
                self.golden_gl_df, 
                self.golden_tp_df, 
                self.bpc_rates_df
            ]
            
            mock_spark = MagicMock()
            mock_spark.read = mock_read
            mock_spark.table = mock_table
            
            result = read_source_data(mock_spark)
            
            self.assertEqual(len(result), 6)
            self.assertIn('FAGLFLEXA', result)
            self.assertIn('BSEG', result)
            self.assertIn('GOLDEN_ENTITY', result)
            self.assertIn('GOLDEN_GL', result)
            self.assertIn('GOLDEN_TRADING_PARTNER', result)
            self.assertIn('BPC_EXCHANGE_RATES', result)

    def test_transform_data(self):
        """Test transform_data function"""
        # Execute the transformation
        result_df = transform_data(self.data_dict)
        
        # Convert to pandas for easier assertions
        result_pd = result_df.toPandas()
        
        # Verify row count (should be 3 after filtering)
        self.assertEqual(len(result_pd), 3)
        
        # Verify columns exist
        expected_columns = [
            "FiscalYear", "PostingPeriod", "PostingDate", "DocumentNumber", 
            "DocumentType", "CompCode", "EntityName", "EntityRegion",
            "RACCT", "GLName", "GLCategory", "PartnerCode", "PartnerName", 
            "PartnerType", "LocalCurrency", "LocalCurrencyAmount",
            "TransactionCurrency", "TransactionCurrencyAmount", "USDAmount"
        ]
        
        for col in expected_columns:
            self.assertIn(col, result_pd.columns)
        
        # Verify specific transformations
        # Check that only RLDNR = '0L' records are included
        self.assertEqual(len(result_pd[result_pd['DocumentNumber'] == '3000000001']), 0)
        
        # Check USD amount calculation
        usd_row = result_pd[result_pd['DocumentNumber'] == '1000000001'].iloc[0]
        self.assertEqual(usd_row['USDAmount'], 1000.0)  # USD to USD conversion
        
        eur_row = result_pd[result_pd['DocumentNumber'] == '1000000002'].iloc[0]
        self.assertEqual(eur_row['USDAmount'], 2000.0 * 1.1)  # EUR to USD conversion

    def test_write_target_data(self):
        """Test write_target_data function"""
        transformed_df = transform_data(self.data_dict)
        
        mock_writer = MagicMock()
        transformed_df.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        
        write_target_data(transformed_df)
        
        mock_writer.mode.assert_called_with("overwrite")
        mock_writer.partitionBy.assert_called_with("FiscalYear", "PostingPeriod")
        mock_writer.parquet.assert_called_once()


if __name__ == '__main__':
    unittest.main()