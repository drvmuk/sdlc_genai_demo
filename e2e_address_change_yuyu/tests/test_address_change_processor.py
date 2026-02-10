"""
Tests for the AddressChangeProcessor class.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from datetime import datetime
import os
import sys

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
from address_change_processor import AddressChangeProcessor


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("AddressChangeProcessorTest") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture(scope="module")
def processor(spark):
    """Create an AddressChangeProcessor instance for testing."""
    return AddressChangeProcessor(spark, "TEST_USER")


@pytest.fixture(scope="module")
def sample_data(spark):
    """Create sample data for testing."""
    # Source data schema
    schema = StructType([
        StructField("T_TX_REQUEST_REQUEST_ID", StringType(), False),
        StructField("T_TX_BASIC_TRANS_ID", StringType(), False),
        StructField("T_TX_RELATION_TRANS_REL_ID", StringType(), False),
        StructField("T_TX_REQ_POL_REQUEST_POLICY_ID", StringType(), False),
        StructField("STG_POLICY_ID", StringType(), False),
        StructField("STG_TXDB_STATUS", StringType(), True),
        StructField("T_TX_REQUEST_ORIGIN_REQUEST_ID", StringType(), True),
        StructField("T_TX_BASIC_POLICY_ID", StringType(), True),
        StructField("T_TX_REQUEST_REQ_ACC_DATETIME", TimestampType(), True),
        StructField("T_TX_DTL_ADD_CH_N_TRANS_ZIP", StringType(), True),
        StructField("T_TX_DTL_ADD_CH_N_TRANS_ADD1", StringType(), True),
        StructField("T_TX_DTL_ADD_CH_N_TRANS_AD1_KJ", StringType(), True),
        StructField("T_TX_DTL_ADD_CH_N_TRANS_ADD2", StringType(), True),
        StructField("T_TX_DTL_ADD_CH_N_TRANS_AD2_KJ", StringType(), True),
        StructField("T_TX_DTL_ADD_CH_N_TRANS_ADD3", StringType(), True),
        StructField("T_TX_DTL_ADD_CH_N_TRANS_AD3_KJ", StringType(), True),
        StructField("T_TX_DTL_ADD_CH_N_TRANS_PHNO", StringType(), True),
        StructField("PPAY_ZIP", StringType(), True),
        StructField("PPAY_ADR1_FW", StringType(), True),
        StructField("PPAY_ADR2_FW", StringType(), True),
        StructField("PPAY_ADR3_FW", StringType(), True),
    ])
    
    # Sample data
    data = [
        # Normal record with all fields
        ("REQ001", "TRANS001", "REL001", "REQPOL001", "POL001", "ACTIVE", "ORIG001", "POL001", 
         datetime.strptime("2023-01-15 10:30:00", "%Y-%m-%d %H:%M:%S"),
         "123-4567", "Tokyo-to", "東京都", "Chiyoda-ku", "千代田区", "1-1-1", "1-1-1",
         "0312345678", "100-0001", "Tokyo-to Backup", "Chiyoda-ku Backup", "1-1-1 Backup"),
        
        # Record with missing ZIP (should use fallback)
        ("REQ002", "TRANS002", "REL002", "REQPOL002", "POL002", "ACTIVE", "ORIG002", "POL002", 
         datetime.strptime("2023-01-16 11:45:00", "%Y-%m-%d %H:%M:%S"),
         None, None, "大阪府", None, "大阪市", None, "1-2-3",
         "0611112222", "530-0001", "Osaka-fu", "Osaka-shi", "1-2-3"),
         
        # Record with mobile phone number
        ("REQ003", "TRANS003", "REL003", "REQPOL003", "POL003", "ACTIVE", "ORIG003", "POL003", 
         datetime.strptime("2023-01-17 14:20:00", "%Y-%m-%d %H:%M:%S"),
         "987-6543", "Kyoto-fu", "京都府", "Kyoto-shi", "京都市", "4-5-6", "4-5-6",
         "09012345678", "600-8001", "Kyoto-fu Backup", "Kyoto-shi Backup", "4-5-6 Backup"),
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    
    # Create lookup tables
    yuyu_clnt_schema = StructType([
        StructField("POL_NO", StringType(), False),
        StructField("POWN_LNM", StringType(), True),
        StructField("POWN_FNM", StringType(), True),
    ])
    
    yuyu_clnt_data = [
        ("POL001", "Tanaka", "Taro"),
        ("POL002", "Suzuki", "Hanako"),
        ("POL003", "Sato", None),
    ]
    
    yuyu_clnt_df = spark.createDataFrame(yuyu_clnt_data, yuyu_clnt_schema)
    
    yuyuk_cln_schema = StructType([
        StructField("POL_NO", StringType(), False),
        StructField("POWN_KNM", StringType(), True),
    ])
    
    yuyuk_cln_data = [
        ("POL001", "田中 太郎"),
        ("POL002", "鈴木 花子"),
        ("POL003", "佐藤"),
    ]
    
    yuyuk_cln_df = spark.createDataFrame(yuyuk_cln_data, yuyuk_cln_schema)
    
    # Register DataFrames as temporary views
    df.createOrReplaceTempView("STG_E2E_AC_TXDBH_DATA")
    yuyu_clnt_df.createOrReplaceTempView("T_YUYU_CLNT")
    yuyuk_cln_df.createOrReplaceTempView("T_YUYUK_CLN")
    
    return {
        "source_df": df,
        "yuyu_clnt_df": yuyu_clnt_df,
        "yuyuk_cln_df": yuyuk_cln_df
    }


def test_read_staging_data(spark, processor, sample_data):
    """Test reading staging data."""
    df = processor.read_staging_data("STG_E2E_AC_TXDBH_DATA")
    assert df.count() == 3
    assert df.filter(df.T_TX_REQUEST_REQUEST_ID == "REQ001").count() == 1


def test_apply_lookups(spark, processor, sample_data):
    """Test applying lookups."""
    source_df = sample_data["source_df"]
    enriched_df = processor.apply_lookups(source_df)
    
    # Check if lookup fields are present
    assert "POWN_LNM" in enriched_df.columns
    assert "POWN_FNM" in enriched_df.columns
    assert "POWN_KNM" in enriched_df.columns
    
    # Check lookup values
    pol001_row = enriched_df.filter(enriched_df.T_TX_BASIC_POLICY_ID == "POL001").first()
    assert pol001_row["POWN_LNM"] == "Tanaka"
    assert pol001_row["POWN_FNM"] == "Taro"
    assert pol001_row["POWN_KNM"] == "田中 太郎"


def test_transform_data(spark, processor, sample_data):
    """Test data transformations."""
    source_df = sample_data["source_df"]
    enriched_df = processor.apply_lookups(source_df)
    transformed_df = processor.transform_data(enriched_df)
    
    # Check transformation fields
    assert "o_NEW_ZIP" in transformed_df.columns
    assert "O_NEW_PHONE_NUMBER" in transformed_df.columns
    assert "O_KANA_NAME" in transformed_df.columns
    
    # Check ZIP normalization (hyphens removed)
    pol001_row = transformed_df.filter(transformed_df.T_TX_BASIC_POLICY_ID == "POL001").first()
    assert pol001_row["o_NEW_ZIP"] == "1234567"
    
    # Check fallback logic for missing ZIP
    pol002_row = transformed_df.filter(transformed_df.T_TX_BASIC_POLICY_ID == "POL002").first()
    assert pol002_row["o_NEW_ZIP"] == "5300001"
    assert pol002_row["o_T_TX_DTL_ADD_CH_N_TRANS_ADD1"] == "Osaka-fu"
    
    # Check phone formatting
    assert pol001_row["O_NEW_PHONE_NUMBER"] == "03-1234-5678"  # Regular phone
    
    pol003_row = transformed_df.filter(transformed_df.T_TX_BASIC_POLICY_ID == "POL003").first()
    assert pol003_row["O_NEW_PHONE_NUMBER"] == "090-1234-5678"  # Mobile phone
    
    # Check name concatenation
    assert pol001_row["O_KANA_NAME"] == "Tanaka Taro"
    assert pol003_row["O_KANA_NAME"] == "Sato"  # Missing first name


def test_prepare_yuyu_extract(spark, processor, sample_data):
    """Test preparation of YUYU extract."""
    source_df = sample_data["source_df"]
    enriched_df = processor.apply_lookups(source_df)
    transformed_df = processor.transform_data(enriched_df)
    yuyu_df = processor.prepare_yuyu_extract(transformed_df)
    
    # Check output columns
    expected_columns = [
        "SEQUENCE_NUMBER", "ORIGIN_REQUEST_ID", "POLICY_ID", "RECEPTION_DATE",
        "NEW_ADDRESS_POSTAL_CODE", "NEW_ADDRESS_KANA_1", "NEW_ADDRESS_KANJI_1",
        "NEW_ADDRESS_KANA_2", "NEW_ADDRESS_KANJI_2", "NEW_ADDRESS_KANA_3",
        "NEW_ADDRESS_KANJI_3", "NEW_ADDRESS_TELEPHONE_NUMBER",
        "POLICY_OWNER_NAME_KANA", "POLICY_OWNER_NAME_KANJI"
    ]
    
    for col in expected_columns:
        assert col in yuyu_df.columns
    
    # Check row count
    assert yuyu_df.count() == 3


def test_prepare_staging_update(spark, processor, sample_data):
    """Test preparation of staging update."""
    source_df = sample_data["source_df"]
    enriched_df = processor.apply_lookups(source_df)
    transformed_df = processor.transform_data(enriched_df)
    update_df = processor.prepare_staging_update(transformed_df)
    
    # Check output columns
    expected_columns = [
        "T_TX_REQUEST_REQUEST_ID", "T_TX_BASIC_TRANS_ID", "T_TX_RELATION_TRANS_REL_ID",
        "T_TX_REQ_POL_REQUEST_POLICY_ID", "STG_POLICY_ID", "STG_TXDB_STATUS",
        "STG_PROCESS_DATE", "STG_PROCESS_USERID"
    ]
    
    for col in expected_columns:
        assert col in update_df.columns
    
    # Check row count
    assert update_df.count() == 3
    
    # Check process user
    first_row = update_df.first()
    assert first_row["STG_PROCESS_USERID"] == "TEST_USER"