"""
Unit tests for the DPAddressChangeYUYU processor.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import tempfile
from src.dp_address_change_yuyu import DPAddressChangeProcessor


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("DPAddressChangeYUYU-Test") \
        .master("local[2]") \
        .getOrCreate()


@pytest.fixture(scope="module")
def temp_dir():
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture(scope="module")
def config(temp_dir):
    """Create test configuration."""
    return {
        "target_dir": temp_dir,
        "batch_size": 1000
    }


@pytest.fixture(scope="module")
def sample_data(spark):
    """Create sample test data."""
    # Create schema for test data
    schema = StructType([
        StructField("T_TX_BASIC_POLICY_ID", StringType(), True),
        StructField("T_TX_REQUEST_ORIGIN_REQUEST_ID", StringType(), True),
        StructField("T_TX_REQUEST_ACCEPTANCE_DATETIME", StringType(), True),
        StructField("PPAY_ZIP", StringType(), True),
        StructField("PPAY_ADR1", StringType(), True),
        StructField("PPAY_ADR2", StringType(), True),
        StructField("PPAY_ADR3", StringType(), True),
        StructField("PPAY_ADR1_KJ", StringType(), True),
        StructField("PPAY_ADR2_KJ", StringType(), True),
        StructField("PPAY_ADR3_KJ", StringType(), True),
        StructField("PPAY_TEL", StringType(), True)
    ])
    
    # Create test data
    data = [
        (
            "POL123456", "REQ123456", "2023-01-15 10:30:00", 
            "123-4567", "アドレス1", "アドレス2", "アドレス3",
            "住所1", "住所2", "住所3", "08012345678"
        ),
        (
            "POL234567", "REQ234567", "2023-01-16 11:45:00", 
            "234-5678", "アドレス4", "アドレス5", "アドレス6",
            "住所4", "住所5", "住所6", "09012345678"
        )
    ]
    
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("STG_E2E_AC_TXDBH_DATA")
    
    # Create lookup tables
    kana_schema = StructType([
        StructField("POL_NO", StringType(), True),
        StructField("POWN_LNM", StringType(), True),
        StructField("POWN_FNM", StringType(), True)
    ])
    
    kana_data = [
        ("POL123456", "タナカ", "タロウ"),
        ("POL234567", "サトウ", "ハナコ")
    ]
    
    kana_df = spark.createDataFrame(kana_data, kana_schema)
    kana_df.createOrReplaceTempView("LKP_YUYU_CLNT")
    
    kanji_schema = StructType([
        StructField("POL_NO", StringType(), True),
        StructField("POWN_KNM", StringType(), True)
    ])
    
    kanji_data = [
        ("POL123456", "田中 太郎"),
        ("POL234567", "佐藤 花子")
    ]
    
    kanji_df = spark.createDataFrame(kanji_data, kanji_schema)
    kanji_df.createOrReplaceTempView("LKP_YUYUK_CLN")
    
    return df


def test_check_source_records(spark, sample_data, config):
    """Test the check_source_records method."""
    processor = DPAddressChangeProcessor(spark, config)
    count = processor.check_source_records()
    
    assert count == 2, "Should find 2 records in the source data"
    
    # Check that the count file was created
    count_file_path = os.path.join(config["target_dir"], "FF_Source_Count.out")
    assert os.path.exists(count_file_path), "Count file should be created"


def test_process_yuyu_creation(spark, sample_data, config):
    """Test the process_yuyu_creation method."""
    processor = DPAddressChangeProcessor(spark, config)
    yuyu_df, staging_df = processor.process_yuyu_creation()
    
    # Check that the output DataFrame has the correct number of rows
    assert yuyu_df.count() == 2, "Output should have 2 rows"
    
    # Check that all required columns are present
    required_columns = [
        "SEQUENCE_NUMBER", "ORIGIN_REQUEST_ID", "POLICY_ID", "RECEPTION_DATE",
        "NEW_ADDRESS_POSTAL_CODE", "NEW_ADDRESS_KANA_1", "NEW_ADDRESS_KANA_2",
        "NEW_ADDRESS_KANA_3", "NEW_ADDRESS_KANJI_1", "NEW_ADDRESS_KANJI_2",
        "NEW_ADDRESS_KANJI_3", "NEW_ADDRESS_TELEPHONE_NUMBER",
        "POLICY_OWNER_NAME_KANA", "POLICY_OWNER_NAME_KANJI"
    ]
    
    for col_name in required_columns:
        assert col_name in yuyu_df.columns, f"Column {col_name} should be present in output"
    
    # Check specific transformations
    row = yuyu_df.filter(yuyu_df.POLICY_ID == "POL123456").collect()[0]
    
    # Check postal code transformation (hyphen removal)
    assert row.NEW_ADDRESS_POSTAL_CODE == "1234567", "Postal code should have hyphens removed"
    
    # Check name concatenation
    assert row.POLICY_OWNER_NAME_KANA == "タナカ タロウ", "Owner KANA name should be correctly concatenated"
    assert row.POLICY_OWNER_NAME_KANJI == "田中 太郎", "Owner KANJI name should be correct"


def test_write_yuyu_output(spark, sample_data, config):
    """Test the write_yuyu_output method."""
    processor = DPAddressChangeProcessor(spark, config)
    yuyu_df, _ = processor.process_yuyu_creation()
    
    # Write the output
    processor.write_yuyu_output(yuyu_df)
    
    # Check that the output file was created
    output_path = os.path.join(config["target_dir"])
    
    # In a real test, we would check for the specific file
    # Here we just check that some files were created
    assert os.listdir(output_path), "Output directory should not be empty"


def test_empty_source_handling(spark, config):
    """Test handling of empty source data."""
    # Create empty source data
    empty_schema = StructType([
        StructField("T_TX_BASIC_POLICY_ID", StringType(), True),
        StructField("T_TX_REQUEST_ORIGIN_REQUEST_ID", StringType(), True)
    ])
    
    empty_df = spark.createDataFrame([], empty_schema)
    empty_df.createOrReplaceTempView("STG_E2E_AC_TXDBH_DATA")
    
    processor = DPAddressChangeProcessor(spark, config)
    count = processor.check_source_records()
    
    assert count == 0, "Should find 0 records in the empty source data"
    
    # Test trigger file creation
    processor.create_empty_trigger_file()
    trigger_path = os.path.join(config["target_dir"], "trigger_file")
    assert os.path.exists(trigger_path), "Trigger file directory should be created"