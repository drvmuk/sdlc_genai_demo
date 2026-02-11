"""
Configuration settings for the E2E Policy Services data extraction pipeline.
"""

# Database connection settings
DB_CONFIG = {
    "source": {
        "jdbc_url": "jdbc:sqlserver://source-server:1433",
        "database": "E2E_POLICYSERVICES",
        "user": "${spark.sql.source.user}",
        "password": "${spark.sql.source.password}",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    },
    "target": {
        "jdbc_url": "jdbc:oracle:thin:@target-server:1521:ORCL",
        "user": "${spark.sql.target.user}",
        "password": "${spark.sql.target.password}",
        "driver": "oracle.jdbc.driver.OracleDriver"
    }
}

# Source tables
SOURCE_TABLES = {
    "T_TX_REQUEST_POLICY": "E2E_POLICYSERVICES.T_TX_REQUEST_POLICY",
    "T_TX_BASIC": "E2E_POLICYSERVICES.T_TX_BASIC",
    "T_TX_RELATION": "E2E_POLICYSERVICES.T_TX_RELATION",
    "T_TX_REQUEST": "E2E_POLICYSERVICES.T_TX_REQUEST",
    "T_TX_DTL_BENEFICIARY_CHANGE": "E2E_POLICYSERVICES.T_TX_DTL_BENEFICIARY_CHANGE"
}

# Target table
TARGET_TABLE = "STG_E2E_BC_K2H_TXDB_DATA"

# Batch size for processing
BATCH_SIZE = 100000

# Default values
DEFAULT_VALUES = {
    "DATA_TYPE": "BENEFICIARY_CHANGE",
    "POLICY_TYPE": "K2H",
    "MC_CRNCY": "JPY"
}