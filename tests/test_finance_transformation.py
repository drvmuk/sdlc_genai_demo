"""
Unit tests for finance data transformation module.
"""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.finance_transformation import transform_finance_data, get_spark_session

@pytest.fixture(scope="session")
def spark():
    """
    Fixture to create a SparkSession for testing.
    """
    return (SparkSession.builder
            .master("local[2]")
            .appName("Finance Data Transformation Tests")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
            .getOrCreate())

@pytest.fixture(scope="function")
def setup_test_data(spark):
    """
    Fixture to set up test data for the tests.
    """
    # Create FAGLFLEXA test data
    faglflexa_data = [
        ("2023", "01", "1000000001", "1000", "0L", "X", "100000", "CC001", "USD", "EUR", 1000.0, 900.0),
        ("2023", "01", "1000000002", "1000", "0L", "X", "200000", "CC002", "USD", "EUR", -500.0, -450.0),
        ("2023", "01", "1000000003", "2000", "0L", "X", "300000", "CC003", "USD", "GBP", 750.0, 600.0),
        ("2023", "02", "1000000004", "2000", "0L", "X", "400000", "CC004", "USD", "GBP", -250.0, -200.0)
    ]
    
    faglflexa_schema = ["RYEAR", "POPER", "DOCNR", "RBUKRS", "RLDNR", "XBILK", 
                        "RACCT", "RCNTR", "RHCUR", "RTCUR", "HSL", "KSL"]
    
    faglflexa_df = spark.createDataFrame(faglflexa_data, faglflexa_schema)
    faglflexa_df.createOrReplaceTempView("FAGLFLEXA")
    
    # Create BSEG test data
    bseg_data = [
        ("1000000001", "1000", "2023", 1000.0, "1000000099"),
        ("1000000002", "1000", "2023", -500.0, "1000000098"),
        ("1000000003", "2000", "2023", 750.0, "1000000097"),
        ("1000000004", "2000", "2023", -250.0, "1000000096")
    ]
    
    bseg_schema = ["BELNR", "BUKRS", "GJAHR", "DMBTR", "AUGBL"]
    
    bseg_df = spark.createDataFrame(bseg_data, bseg_schema)
    bseg_df.createOrReplaceTempView("BSEG")
    
    # Create Entity view test data
    entity_data = [
        ("1000", "Entity1"),
        ("2000", "Entity2")
    ]
    
    entity_schema = ["CompanyCode", "LegalEntity"]
    
    entity_df = spark.createDataFrame(entity_data, entity_schema)
    entity_df.createOrReplaceTempView("v_entity")
    
    # Create GL Account view test data
    gl_data = [
        ("100000", "GL100"),
        ("200000", "GL200"),
        ("300000", "GL300"),
        ("400000", "GL400"),
        ("999001", "GL901"),
        ("999002", "GL902"),
        ("999003", "GL903"),
        ("999004", "GL904")
    ]
    
    gl_schema = ["GLAccount", "GoldenGLAcct"]
    
    gl_df = spark.createDataFrame(gl_data, gl_schema)
    gl_df.createOrReplaceTempView("v_gl_account")
    
    # Create Trading Partner view test data
    tp_data = [
        ("CC001", "TP001"),
        ("CC002", "TP002"),
        ("CC003", "TP003"),
        ("CC004", "TP004")
    ]
    
    tp_schema = ["TradingPartner", "GoldenTradingPartner"]
    
    tp_df = spark.createDataFrame(tp_data, tp_schema)
    tp_df.createOrReplaceTempView("v_trading_partner")
    
    # Create Realized/Unrealized view test data
    ru_data = [
        ("100000", "Realized"),
        ("200000", "Realized"),
        ("300000", "Unrealized"),
        ("400000", "Unrealized")
    ]
    
    ru_schema = ["GLAccount", "Realized_Unrealized"]
    
    ru_df = spark.createDataFrame(ru_data, ru_schema)
    ru_df.createOrReplaceTempView("v_realized_unrealized_glaccts")
    
    # Create BPC Exchange Rates view test data
    er_data = [
        ("2023", "01", "USD", "USD", 1.0),
        ("2023", "01", "EUR", "USD", 1.1),
        ("2023", "01", "GBP", "USD", 1.3),
        ("2023", "02", "USD", "USD", 1.0),
        ("2023", "02", "EUR", "USD", 1.12),
        ("2023", "02", "GBP", "USD", 1.32)
    ]
    
    er_schema = ["Year", "Period", "FromCurrency", "ToCurrency", "ExchangeRate"]
    
    er_df = spark.createDataFrame(er_data, er_schema)
    er_df.createOrReplaceTempView("v_bpc_exchange_rates")
    
    # Register all tables in the spark catalog
    spark.sql("CREATE DATABASE IF NOT EXISTS everest_ecc")
    spark.sql("CREATE DATABASE IF NOT EXISTS golden_views")
    
    faglflexa_df.write.mode("overwrite").saveAsTable("everest_ecc.FAGLFLEXA")
    bseg_df.write.mode("overwrite").saveAsTable("everest_ecc.BSEG")
    entity_df.write.mode("overwrite").saveAsTable("golden_views.v_entity")
    gl_df.write.mode("overwrite").saveAsTable("golden_views.v_gl_account")
    tp_df.write.mode("overwrite").saveAsTable("golden_views.v_trading_partner")
    ru_df.write.mode("overwrite").saveAsTable("golden_views.v_realized_unrealized_glaccts")
    er_df.write.mode("overwrite").saveAsTable("golden_views.v_bpc_exchange_rates")
    
    yield
    
    # Clean up
    spark.sql("DROP DATABASE IF EXISTS everest_ecc CASCADE")
    spark.sql("DROP DATABASE IF EXISTS golden_views CASCADE")

def test_transform_finance_data(spark, setup_test_data):
    """
    Test the finance data transformation logic.
    """
    # Execute the transformation
    result_df = transform_finance_data(spark)
    
    # Check that the result is not empty
    assert result_df.count() > 0
    
    # Check that all required columns are present
    expected_columns = [
        "FiscalYear", "PostingPeriod", "DocumentNumber", "CompCode", "LegalEntity",
        "GLAccount", "GoldenGLAcct", "TradingPartner", "GoldenTradingPartner",
        "GainLossGC", "GainLossLC", "GainLossTC", "LocalCurrency", "TransactionCurrency",
        "OffsetAccount", "GoldenOffsetAccount", "OffsetAccountLCAmount",
        "OffsetClearingDocumentNumber", "SourceSystem"
    ]
    
    for column in expected_columns:
        assert column in result_df.columns
    
    # Check specific transformations
    # 1. Check that GainLossGC is calculated correctly
    result_df.createOrReplaceTempView("result")
    
    # For account 100000 with positive LC amount, GainLossGC should be 1000.0 * 1.1 = 1100.0
    positive_realized = result_df.filter(
        (F.col("GLAccount") == "100000") & 
        (F.col("DocumentNumber") == "1000000001")
    ).collect()
    
    assert len(positive_realized) == 1
    assert abs(positive_realized[0]["GainLossGC"] - 1100.0) < 0.01
    assert positive_realized[0]["OffsetAccount"] == "999001"  # Positive realized
    
    # For account 200000 with negative LC amount, GainLossGC should be -500.0 * 1.1 = -550.0
    negative_realized = result_df.filter(
        (F.col("GLAccount") == "200000") & 
        (F.col("DocumentNumber") == "1000000002")
    ).collect()
    
    assert len(negative_realized) == 1
    assert abs(negative_realized[0]["GainLossGC"] + 550.0) < 0.01
    assert negative_realized[0]["OffsetAccount"] == "999002"  # Negative realized
    
    # For account 300000 with positive LC amount, GainLossGC should be 750.0 * 1.3 = 975.0
    positive_unrealized = result_df.filter(
        (F.col("GLAccount") == "300000") & 
        (F.col("DocumentNumber") == "1000000003")
    ).collect()
    
    assert len(positive_unrealized) == 1
    assert abs(positive_unrealized[0]["GainLossGC"] - 975.0) < 0.01
    assert positive_unrealized[0]["OffsetAccount"] == "999003"  # Positive unrealized
    
    # For account 400000 with negative LC amount, GainLossGC should be -250.0 * 1.32 = -330.0
    negative_unrealized = result_df.filter(
        (F.col("GLAccount") == "400000") & 
        (F.col("DocumentNumber") == "1000000004")
    ).collect()
    
    assert len(negative_unrealized) == 1
    assert abs(negative_unrealized[0]["GainLossGC"] + 330.0) < 0.01
    assert negative_unrealized[0]["OffsetAccount"] == "999004"  # Negative unrealized
    
    # Check that all records have SourceSystem set to "Everest ECC"
    source_system_count = result_df.filter(F.col("SourceSystem") == "Everest ECC").count()
    assert source_system_count == result_df.count()

def test_get_spark_session():
    """
    Test that get_spark_session returns a valid SparkSession.
    """
    spark = get_spark_session()
    assert spark is not None
    assert spark.version is not None