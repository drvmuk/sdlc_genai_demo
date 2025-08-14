"""
Unit tests for the Finance Data Processor
"""

import os
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from src.finance_processor import FinanceDataProcessor

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("FinanceDataProcessorTest") \
        .master("local[2]") \
        .getOrCreate()

@pytest.fixture
def sample_data(spark):
    """Create sample test data"""
    
    # Create FAGLFLEXA sample data
    faglflexa_schema = StructType([
        StructField("RLDNR", StringType(), False),
        StructField("DOCNR", StringType(), False),
        StructField("RBUKRS", StringType(), False),
        StructField("RYEAR", StringType(), False),
        StructField("RACCT", StringType(), False),
        StructField("RASSC", StringType(), False),
        StructField("RWCUR", StringType(), False),
        StructField("HSL", DoubleType(), False),
        StructField("TSL", DoubleType(), False),
    ])
    
    faglflexa_data = [
        ("0L", "1000000001", "1000", "2023", "400000", "2000", "USD", 1000.0, 1000.0),
        ("0L", "1000000002", "2000", "2023", "500000", "3000", "EUR", 2000.0, 2500.0),
        ("0L", "1000000003", "3000", "2023", "600000", "4000", "GBP", 3000.0, 3500.0),
        ("0L", "1000000004", "8000", "2023", "700000", "5000", "JPY", 4000.0, 4500.0),  # Should be filtered out
        ("1L", "1000000005", "5000", "2023", "800000", "6000", "USD", 5000.0, 5500.0),  # Should be filtered out
    ]
    
    faglflexa_df = spark.createDataFrame(faglflexa_data, faglflexa_schema)
    
    # Create BSEG sample data
    bseg_schema = StructType([
        StructField("BELNR", StringType(), False),
        StructField("BUKRS", StringType(), False),
        StructField("GJAHR", StringType(), False),
        StructField("XBILK", StringType(), False),
        StructField("HKONT", StringType(), False),
        StructField("DMBTR", DoubleType(), False),
        StructField("WRBTR", DoubleType(), False),
        StructField("AUGBL", StringType(), True),
    ])
    
    bseg_data = [
        ("1000000001", "1000", "2023", "X", "100000", 1000.0, 1000.0, "2000000001"),
        ("1000000002", "2000", "2023", "X", "200000", 2000.0, 2500.0, "2000000002"),
        ("1000000003", "3000", "2023", "X", "300000", 3000.0, 3500.0, "2000000003"),
        ("1000000004", "8000", "2023", "X", "400000", 4000.0, 4500.0, "2000000004"),
        ("1000000005", "5000", "2023", "", "500000", 5000.0, 5500.0, "2000000005"),  # Should be filtered out
    ]
    
    bseg_df = spark.createDataFrame(bseg_data, bseg_schema)
    
    # Create Entity View sample data
    entity_schema = StructType([
        StructField("company_code", StringType(), False),
        StructField("golden_entity", StringType(), False),
    ])
    
    entity_data = [
        ("1000", "ENT1000"),
        ("2000", "ENT2000"),
        ("3000", "ENT3000"),
        ("4000", "ENT4000"),
        ("5000", "ENT5000"),
    ]
    
    entity_df = spark.createDataFrame(entity_data, entity_schema)
    
    # Create GL View sample data
    gl_schema = StructType([
        StructField("gl_account", StringType(), False),
        StructField("golden_gl", StringType(), False),
    ])
    
    gl_data = [
        ("400000", "GL400000"),
        ("500000", "GL500000"),
        ("600000", "GL600000"),
        ("700000", "GL700000"),
        ("100000", "GL100000"),
        ("200000", "GL200000"),
        ("300000", "GL300000"),
    ]
    
    gl_df = spark.createDataFrame(gl_data, gl_schema)
    
    # Create Trading Partner View sample data
    tp_schema = StructType([
        StructField("trading_partner", StringType(), False),
        StructField("golden_trading_partner", StringType(), False),
    ])
    
    tp_data = [
        ("2000", "TP2000"),
        ("3000", "TP3000"),
        ("4000", "TP4000"),
        ("5000", "TP5000"),
        ("6000", "TP6000"),
    ]
    
    tp_df = spark.createDataFrame(tp_data, tp_schema)
    
    # Create Exchange Rate sample data
    exchange_rate_schema = StructType([
        StructField("from_currency", StringType(), False),
        StructField("to_currency", StringType(), False),
        StructField("rate", DoubleType(), False),
        StructField("effective_date", StringType(), False),
    ])
    
    exchange_rate_data = [
        ("USD", "EUR", 0.85, "2023-01-01"),
        ("EUR", "USD", 1.18, "2023-01-01"),
        ("GBP", "USD", 1.38, "2023-01-01"),
        ("USD", "JPY", 110.0, "2023-01-01"),
    ]
    
    exchange_rate_df = spark.createDataFrame(exchange_rate_data, exchange_rate_schema)
    
    # Create Revenue Entity sample data
    revenue_entity_schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("local_currency", StringType(), False),
    ])
    
    revenue_entity_data = [
        ("ENT1000", "USD"),
        ("ENT2000", "EUR"),
        ("ENT3000", "GBP"),
        ("ENT4000", "JPY"),
        ("ENT5000", "USD"),
    ]
    
    revenue_entity_df = spark.createDataFrame(revenue_entity_data, revenue_entity_schema)
    
    return {
        "FAGLFLEXA": faglflexa_df,
        "BSEG": bseg_df,
        "ENTITY_VIEW": entity_df,
        "GL_VIEW": gl_df,
        "TRADING_PARTNER_VIEW": tp_df,
        "EXCHANGE_RATE": exchange_rate_df,
        "REVENUE_ENTITY": revenue_entity_df
    }

class TestFinanceDataProcessor:
    """Test cases for the Finance Data Processor"""
    
    def test_join_source_tables(self, spark, sample_data, monkeypatch):
        """Test joining source tables"""
        # Create processor instance
        processor = FinanceDataProcessor(spark, "2023", "12")
        
        # Mock the _read_source_data method
        monkeypatch.setattr(processor, "_read_source_data", lambda: sample_data)
        
        # Call the join method
        joined_df = processor._join_source_tables(sample_data)
        
        # Verify the join worked correctly
        assert joined_df.count() == 4
        
        # Check that the join includes records that match on all three join conditions
        result = joined_df.filter(
            (F.col("DOCNR") == "1000000001") & 
            (F.col("RBUKRS") == "1000") & 
            (F.col("RYEAR") == "2023")
        ).count()
        assert result == 1
    
    def test_transform_data(self, spark, sample_data, monkeypatch):
        """Test data transformation logic"""
        # Create processor instance
        processor = FinanceDataProcessor(spark, "2023", "12")
        
        # Mock the _read_source_data method
        monkeypatch.setattr(processor, "_read_source_data", lambda: sample_data)
        
        # Get joined data
        joined_df = processor._join_source_tables(sample_data)
        
        # Transform the data
        transformed_df = processor._transform_data(joined_df, sample_data)
        
        # Verify transformation results
        assert transformed_df.count() == 3