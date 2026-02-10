"""
Configuration settings for E2E Policy Services Control processes
"""

# Oracle connection configuration
ORACLE_CONFIG = {
    "driver": "oracle.jdbc.driver.OracleDriver",
    "url": "jdbc:oracle:thin:@//oracle-host:1521/service_name",
    "user": "ZSYSE2EDEV",
    # Password should be stored in a secure location like Databricks Secrets
    "password_scope": "oracle",
    "password_key": "password"
}

# Default output configuration
DEFAULT_OUTPUT_CONFIG = {
    "base_path": "/mnt/control/source_counts/",
    "file_format": "csv",
    "delimiter": ",",
    "include_header": False
}

# Logging configuration
LOG_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
}