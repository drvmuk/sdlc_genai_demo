"""
Configuration module for E2E Policy Services YUYU extract.
Manages database connections, parameters, and paths.
"""

import os
from typing import Dict, Any


class Config:
    """Configuration class for the YUYU extract pipeline."""
    
    # Oracle connection parameters
    ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
    ORACLE_PORT = os.getenv("ORACLE_PORT", "1521")
    ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "ORCL")
    ORACLE_USER = os.getenv("ORACLE_USER", "ZSYSE2EDEV")
    ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "password")
    
    # Processing parameters
    M_PROCESS_USERID = os.getenv("M_PROCESS_USERID", "ETL_USER")
    
    # Output paths
    OUTPUT_BASE_PATH = os.getenv("OUTPUT_PATH", "/tmp/yuyu_output")
    FLAT_FILE_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/DPAddressChangeYUYU"
    
    # Table names
    SOURCE_TABLE = "STG_E2E_AC_TXDBH_DATA"
    LOOKUP_TABLE_YUYU_CLNT = "T_YUYU_CLNT"
    LOOKUP_TABLE_YUYUK_CLN = "T_YUYUK_CLN"
    
    # Sequence generator settings
    SEQUENCE_START = 0
    SEQUENCE_INCREMENT = 1
    
    # Output format
    OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "parquet")  # parquet or csv
    
    @classmethod
    def get_oracle_jdbc_url(cls) -> str:
        """Construct Oracle JDBC URL."""
        return f"jdbc:oracle:thin:@{cls.ORACLE_HOST}:{cls.ORACLE_PORT}/{cls.ORACLE_SERVICE}"
    
    @classmethod
    def get_oracle_properties(cls) -> Dict[str, str]:
        """Get Oracle connection properties."""
        return {
            "user": cls.ORACLE_USER,
            "password": cls.ORACLE_PASSWORD,
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
    
    @classmethod
    def get_spark_config(cls) -> Dict[str, Any]:
        """Get Spark configuration."""
        return {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.session.timeZone": "Asia/Tokyo"
        }