import pytest
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

from src.transformations import apply_address_change_transformations

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing"""
    return (SparkSession.builder
            .master("local[2]")
            .appName("E2E Address Change YUYU Tests")
            .getOrCreate())

@pytest.fixture(scope="module")
def sample_data(spark):
    """Create sample DataFrames for testing"""
    # Sample source data
    source_schema = StructType([
        StructField("T_TX_REQUEST_REQUEST_ID", StringType(), False),
        StructField("T_TX_BASIC_TRANS_ID", StringType(), False),
        StructField("T_TX_RELATION_TRANS_REL_ID", StringType(), False),
        StructField("T_TX_REQ_POL_REQUEST_POLICY_ID", StringType(), False),
        StructField("STG_POLICY_ID", StringType(), False),
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
        StructField("STG_TXDB_STATUS", StringType(), True),
        StructField("STG_PROCESS_DATE", TimestampType(), True),
        StructField("STG_PROCESS_USERID", StringType(), True)
    ])
    
    source_data = [
        # Record 1: Complete data with ZIP
        (
            "REQ001", "TRANS001", "REL001", "REQPOL001", "POL001", 
            "ORIG001", "12345678", datetime.datetime(2023, 5, 1, 10, 30, 0),
            "123-4567", "東京都渋谷区", "東京都渋谷区", "代々木1-1-1", "代々木1-1-1", "", "", 
            "0312345678", "999-9999", "Fallback Addr1", "Fallback Addr2", "Fallback Addr3",
            "PENDING", None, None
        ),
        # Record 2: Missing ZIP, should use fallback
        (
            "REQ002", "TRANS002", "REL002", "REQPOL002", "POL002", 
            "ORIG002", "87654321", datetime.datetime(2023, 5, 2, 11, 45, 0),
            None, None, "大阪府大阪市", None, "中央区1-2-3", None, "梅田ビル4F", 
            "0611112222", "888-8888", "大阪府大阪市", "中央区1-2-3", "梅田ビル4F",
            "PENDING", None