"""
Unit tests for the Finance Data Processor module.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime

from src.finance_data_processor import FinanceDataProcessor


class TestFinanceDataProcessor(unittest.TestCase):
    """Test cases for the Finance Data Processor."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the SparkSession for all test cases."""
        cls.spark = SparkSession.builder \
            .appName("Finance Data Processor Tests") \
            .master("local[2]") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop the SparkSession after all tests."""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test case."""
        # Create sample data for testing
        
        # FAGLFLEXA sample data
        faglflexa_schema = StructType([
            StructField("RYEAR", StringType(), False),
            StructField("POPER", StringType(), False),
            StructField("DOCNR", StringType(), False),
            StructField("RBUKRS", StringType(), False),
            StructField("RACCT", StringType(), False),
            StructField("RCNTR", StringType(), True),
            StructField("PRCTR", StringType(), True),
            StructField("RFAREA", StringType(), True),
            StructField("RBUSA", StringType(), True),
            StructField("KOKRS", StringType(), True),
            StructField("SEGMENT", StringType(), True),
            StructField("SCNTR", StringType(), True),
            StructField("PPRCTR", StringType(), True),
            StructField("SFAREA", StringType(), True),
            StructField("SBUSA", StringType(), True),
            StructField("RASSC", StringType(), True),
            StructField("HSLVT", DecimalType(17, 2), True),
            StructField("HSL", DecimalType(17, 2), True),
            StructField("RHCUR", StringType(), True),
            StructField("RKCUR", StringType(), True),
            StructField("KSLVT", DecimalType(17, 2), True),
            StructField("KSL", DecimalType(17, 2), True),
            StructField("DRCRK", StringType(), True),
        ])
        
        self.faglflexa_data = [
            ("2023", "12", "1000000001", "1000", "100000", "CC001", "PC001", "FA001", "BA001", "CO01", 
             "SEG01", "SCC001", "SPC001", "SFA001", "SBA001", "TP001", 1000.00, 5000.00, "USD", "EUR", 
             900.00, 4500.00, "S"),
            ("2023", "12", "1000000001", "1000", "200000", "CC002", "PC002", "FA002", "BA002", "CO01", 
             "SEG01", "SCC002", "SPC002", "SFA002", "SBA002", "TP002", -1000.00, -5000.00, "USD", "EUR", 
             -900.00, -4500.00, "H"),
            ("2023", "12", "1000000002", "2000", "100000", "CC003", "PC003", "FA003", "BA003", "CO02", 
             "SEG02", "SCC003", "SPC003", "SFA003", "SBA003", "TP001", 2000.00, 8000.00, "JPY", "USD", 
             18.00, 72.00, "S"),
        ]
        
        self.faglflexa_df = self.spark.createDataFrame(self.faglflexa_data, faglflexa_schema)
        
        # BSEG sample data
        bseg_schema = StructType([
            StructField("GJAHR", StringType(), False),
            StructField("MONAT", StringType(), False),
            StructField("BELNR", StringType(), False),
            StructField("BUZEI", StringType(), False),
            StructField("BUKRS", StringType(), False),
            StructField("HKONT", StringType(), False),
            StructField("AUGDT", DateType(), True),
            StructField("AUGBL", StringType(), True),
            StructField("ZUONR", StringType(), True),
            StructField("SGTXT", StringType(), True