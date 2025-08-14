"""
Tests for Finance Data Processor
"""
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.finance_processor import FinanceDataProcessor

@pytest.fixture(scope="module")
def spark():
    """
    Create a SparkSession for testing.
    """
    return SparkSession.builder \
        .appName("Finance Data Processor Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture(scope="module")
def test_config():
    """
    Test configuration for the Finance Data Processor.
    """
    return {
        # Test paths
        "faglflexa_path": "data/test/FAGLFLEXA",
        "bseg_path": "data/test/BSEG",
        "entity_view_path": "data/test/Entity",
        "gl_view_path": "data/test/GL",
        "trading_partner_view_path": "data/test/TradingPartner",
        "exchange_rate_path": "data/test/exchange_rate",
        
        # Target path
        "finance_table_path": "data/test/Finance",
        
        # Error log path
        "error_log_path": "data/test/error_log",
        
        # Business logic configuration
        "realized_accounts": ["40001", "40002", "40003"],
        "unrealized_accounts": ["50001", "50002", "50003"],
        "realized_offset_account": "45000",
        "unrealized_offset_account": "55000"
    }


@pytest.fixture(scope="module")
def sample_data(spark):
    """
    Create sample data for testing.
    """
    # FAGLFLEXA sample data
    faglflexa_data = [
        ("1000000001", "1000", "2023", "01", "0L", "X", "40001", "2000", 1000.0),
        ("1000000002", "1000", "2023", "01", "0L", "X", "50001", "3000", 2000.0),
        ("1000000003", "2000", "2023", "01", "0L", "X", "40002", "4000", 3000.0),
        ("1000000004", "8000", "2023", "01", "0L", "X", "60001", "5000", 4000.0),  # Should be filtered out
        ("1000000005", "3000", "2023", "01", "0L", "X", "70001", "6000", 5000.0),
        ("1000000006", "3000", "2023", "01", "1L", "X", "70001", "6000", 6000.0),  # Should be filtered out
    ]
    
    faglflexa_schema = StructType([
        StructField("DOCNR", StringType(), True),
        StructField("RBUKRS", StringType(), True),
        StructField("RYEAR", StringType(), True),
        StructField("POPER", StringType(), True),
        StructField("RLDNR", StringType(), True),
        StructField("XBILK", StringType(), True),
        StructField("RACCT", StringType(), True),
        StructField("RASSC", StringType(), True),
        StructField("HSL", DoubleType(), True)
    ])
    
    faglflexa_df = spark.createDataFrame(faglflexa_data, faglflexa_schema)
    
    # BSEG sample data
    bseg_data = [
        ("1000000001", "1000", "2023", "USD", "EUR"),
        ("1000000002", "1000", "2023", "EUR", "USD"),
        ("1000000003", "2000", "2023", "GBP", "EUR"),
        ("1000000004", "8000", "2023", "JPY", "USD"),
        ("1000000005", "3000", "2023", "EUR", "GBP"),
    ]
    
    bseg_schema = StructType([
        StructField("BELNR", StringType(), True),
        StructField("BUKRS", StringType(), True),
        StructField("GJAHR", StringType(), True),
        StructField("LocalCurrency", StringType(), True),
        StructField("TransactionalCurrency", StringType(), True)
    ])
    
    bseg_df = spark.createDataFrame(bseg_data, bseg_schema)
    
    # Entity sample data
    entity_data = [
        ("1000", "ENTITY1", "N"),
        ("2000", "ENTITY2", "N"),
        ("3000", "ENTITY3", "Y"),  # Should be filtered out
        ("8000", "ENTITY8", "N"),
    ]
    
    entity_schema = StructType([
        StructField("CompanyCode", StringType(), True),
        StructField("Entity", StringType(), True),
        StructField("HistEntity", StringType(), True)
    ])
    
    entity_df = spark.createDataFrame(entity_data, entity_schema)
    
    # GL sample data
    gl_data = [
        ("40001", "GL40001"),
        ("40002", "GL40002"),
        ("40003", "GL40003"),
        ("50001", "GL50001"),
        ("50002", "GL50002"),
        ("50003", "GL50003"),
        ("60001", "GL60001"),
        ("70001", "GL70001"),
        ("45000", "GL45000"),
        ("55000", "GL55000")
    ]
    
    gl_schema = StructType([
        StructField("SourceGLAccount", StringType(), True),
        StructField("GoldenGLAccount", StringType(), True)
    ])
    
    gl_df = spark.createDataFrame(gl_data, gl_schema)
    
    # Trading Partner sample data
    tp_data = [
        ("2000", "TP2000"),
        ("3000", "TP3000"),
        ("4000", "TP4000"),
        ("5000", "TP5000"),
        ("6000", "TP6000")
    ]
    
    tp_schema = StructType([
        StructField("SourceTradingPartner", StringType(), True),
        StructField("GoldenTradingPartner", StringType(), True)
    ])
    
    tp_df = spark.createDataFrame(tp_data, tp_schema)
    
    # Exchange Rate sample data
    er_data = [
        ("1000", "2023", "01", 1.1),
        ("2000", "2023", "01", 0.9),
        ("3000", "2023", "01", 1.2),
        ("8000", "2023", "01", 0.8)
    ]
    
    er_schema = StructType([
        StructField("CompanyCode", StringType(), True),
        StructField("FiscalYear", StringType(), True),
        StructField("Period", StringType(), True),
        StructField("ExchangeRate", DoubleType(), True)
    ])
    
    er_df = spark.createDataFrame(er_data, er_schema)
    
    # Add currency rates
    er_df = er_df.withColumn("LocalCurrencyRate", F.lit(1.0))
    er_df = er_df.withColumn("TransactionalCurrencyRate", F.lit(1.05))
    
    return {
        "faglflexa_df": faglflexa_df,
        "bseg_df": bseg_df,
        "entity_df": entity_df,
        "gl_df": gl_df,
        "trading_partner_df": tp_df,
        "exchange_rate_df": er_df
    }


@pytest.fixture(scope="module")
def processor(spark, test_config, sample_data):
    """
    Create a FinanceDataProcessor instance for testing.
    """
    processor = FinanceDataProcessor(spark, test_config)
    
    # Mock the read_source_data method to return sample data
    processor.read_source_data = lambda: (
        sample_data["faglflexa_df"],
        sample_data["bseg_df"],
        sample_data["entity_df"],
        sample_data["gl_df"],
        sample_data["trading_partner_df"],
        sample_data["exchange_rate_df"]
    )
    
    # Mock the _write_to_target method to do nothing
    processor._write_to_target = lambda df: None
    
    # Mock the _log_error method to do nothing
    processor._log_error = lambda function_name, error_message: None
    
    return processor


def test_join_faglflexa_bseg(processor, sample_data):
    """
    Test joining FAGLFLEXA with BSEG.
    """
    result = processor._join_faglflexa_bseg(
        sample_data["faglflexa_df"],
        sample_data["bseg_df"]
    )
    
    # Check that only records with RLDNR = '0L' and XBILK = 'X' are included
    assert result.filter(
        (F.col("RLDNR") != "0L") | (F.col("XBILK") != "X")
    ).count() == 0
    
    # Check that join was successful
    assert result.filter(F.col("LocalCurrency").isNotNull()).count() > 0


def test_filter_records(processor, sample_data):
    """
    Test filtering records based on CompCode and HistEntity.
    """
    joined_df = processor._join_faglflexa_bseg(
        sample_data["faglflexa_df"],
        sample_data["bseg_df"]
    )
    
    result = processor._filter_records(joined_df, sample_data["entity_df"])
    
    # Check that records with CompCode starting with '8%' are filtered out
    assert result.filter(F.col("RBUKRS").like("8%")).count() == 0
    
    # Check that records with HistEntity = 'Y' are filtered out
    assert result.join(
        sample_data["entity_df"].filter(F.col("HistEntity") == "Y"),
        result.RBUKRS == sample_data["entity_df"].CompanyCode,
        "inner"
    ).count() == 0


def test_convert_to_golden_values(processor, sample_data):
    """
    Test converting source values to Golden values.
    """
    joined_df = processor._join_faglflexa_bseg(
        sample_data["faglflexa_df"],
        sample_data["bseg_df"]
    )
    
    filtered_df = processor._filter_records(joined_df, sample_data["entity_df"])
    
    result = processor._convert_to_golden_values(
        filtered_df,
        sample_data["entity_df"],
        sample_data["gl_df"],
        sample_data["trading_partner_df"]
    )
    
    # Check that Golden values are populated
    assert result.filter(F.col("GoldenEntity").isNotNull()).count() > 0
    assert result.filter(F.col("GoldenGL").isNotNull()).count() > 0
    assert result.filter(F.col("GoldenTradingPartner").isNotNull()).count() > 0


def test_calculate_gain_loss(processor, sample_data):
    """
    Test calculating Gain/Loss.
    """
    joined_df = processor._join_faglflexa_bseg(
        sample_data["faglflexa_df"],
        sample_data["bseg_df"]
    )
    
    filtered_df = processor._filter_records(joined_df, sample_data["entity_df"])
    
    golden_df = processor._convert_to_golden_values(
        filtered_df,
        sample_data["entity_df"],
        sample_data["gl_df"],
        sample_data["trading_partner_df"]
    )
    
    result = processor._calculate_gain_loss(golden_df, sample_data["exchange_rate_df"])
    
    # Check that Gain/Loss columns are populated
    assert result.filter(F.col("GainLossGC").isNotNull()).count() > 0
    assert result.filter(F.col("GainLossLC").isNotNull