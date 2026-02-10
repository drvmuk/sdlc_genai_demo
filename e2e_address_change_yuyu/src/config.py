import os
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Database connection parameters
JDBC_URL = os.environ.get("JDBC_URL", "jdbc:oracle:thin:@//oracle-host:1521/ZSYSE2EDEV")
DB_USER = os.environ.get("DB_USER", "e2e_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
PROCESS_USERID = os.environ.get("PROCESS_USERID", "E2E_BATCH")

# Oracle table names
SOURCE_TABLE = "STG_E2E_AC_TXDBH_DATA"
YUYU_CLNT_TABLE = "T_YUYU_CLNT"
YUYUK_CLN_TABLE = "T_YUYUK_CLN"

# Output file path
OUTPUT_FILE_PATH = "/dbfs/mnt/e2e_policy_services/output/DPAddressChangeYUYU"

# Schema definitions
STG_E2E_AC_TXDBH_DATA_SCHEMA = StructType([
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

YUYU_CLNT_SCHEMA = StructType([
    StructField("POL_NO", StringType(), True),
    StructField("POWN_LNM", StringType(), True),
    StructField("POWN_FNM", StringType(), True)
])

YUYUK_CLN_SCHEMA = StructType([
    StructField("POL_NO", StringType(), True),
    StructField("POWN_KNM", StringType(), True)
])

DP_ADDRESS_CHANGE_YUYU_SCHEMA = StructType([
    StructField("SEQUENCE_NUMBER", IntegerType(), False),
    StructField("ORIGIN_REQUEST_ID", StringType(), True),
    StructField("POLICY_ID", StringType(), True),
    StructField("RECEPTION_DATE", TimestampType(), True),
    StructField("NEW_ADDRESS_POSTAL_CODE", StringType(), True),
    StructField("NEW_ADDRESS_KANA_1", StringType(), True),
    StructField("NEW_ADDRESS_KANJI_1", StringType(), True),
    StructField("NEW_ADDRESS_KANA_2", StringType(), True),
    StructField("NEW_ADDRESS_KANJI_2", StringType(), True),
    StructField("NEW_ADDRESS_KANA_3", StringType(), True),
    StructField("NEW_ADDRESS_KANJI_3", StringType(), True),
    StructField("NEW_ADDRESS_TELEPHONE_NUMBER", StringType(), True),
    StructField("POLICY_OWNER_NAME_KANA", StringType(), True),
    StructField("POLICY_OWNER_NAME_KANJI", StringType(), True)
])

# JDBC connection properties
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "oracle.jdbc.driver.OracleDriver"
}