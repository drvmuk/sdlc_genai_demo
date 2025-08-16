"""
Configuration module for Finance Data Processing.

This module contains configuration settings for the Finance Data Processing job.
"""

# Cluster configuration
CLUSTER_CONFIG = {
    "cluster_name": "Finance Data Processing Cluster",
    "spark_version": "7.3 LTS",
    "node_type_id": "Standard_DS3_v2",
    "driver_node_type_id": "Standard_DS3_v2",
    "min_workers": 2,
    "max_workers": 5,
    "autoscale": True,
    "auto_termination_minutes": 30
}

# Database connection settings
# In a real implementation, these would be stored securely and retrieved at runtime
DB_CONFIG = {
    "ecc_everest": {
        "url": "jdbc:sqlserver://ecc-everest-server:1433;databaseName=ECC_Everest",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": "${ECC_DB_USER}",  # Environment variable
        "password": "${ECC_DB_PASSWORD}"  # Environment variable
    },
    "data_warehouse": {
        "url": "jdbc:sqlserver://data-warehouse-server:1433;databaseName=DataWarehouse",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": "${DW_DB_USER}",  # Environment variable
        "password": "${DW_DB_PASSWORD}"  # Environment variable
    }
}

# Source table/view configurations
SOURCE_CONFIG = {
    "faglflexa": {
        "table": "FAGLFLEXA",
        "database": "ecc_everest"
    },
    "bseg": {
        "table": "BSEG",
        "database": "ecc_everest"
    },
    "entity_golden_view": {
        "table": "v_entity_golden",
        "database": "data_warehouse"
    },
    "golden_gl_view": {
        "table": "v_golden_gl",
        "database": "data_warehouse"
    },
    "golden_trading_partner_view": {
        "table": "v_golden_trading_partner",
        "database": "data_warehouse"
    },
    "bpc_exchange_rates": {
        "table": "s_shared.v_actual_exchange_rate_bpc",
        "database": "data_warehouse"
    }
}

# Target table configuration
TARGET_CONFIG = {
    "finance_table": {
        "table": "Finance",
        "database": "data_warehouse",
        "mode": "append"  # or "overwrite" depending on requirements
    }
}

# Constants for data processing
LOCAL_CURRENCY_MULT_FACTOR = ["JPY", "KRW", "IDR", "CLP", "COP", "VND"]
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30