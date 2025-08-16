"""
Configuration settings for the Finance Data Processor
"""

# Source data paths
SOURCE_PATHS = {
    'FAGLFLEXA': '/everest_ecc/faglflexta',
    'BSEG': '/everest_ecc/bseg',
    'GOLDEN_ENTITY': '/golden/entity',
    'GOLDEN_GL': '/golden/gl',
    'GOLDEN_TRADING_PARTNER': '/golden/trading_partner',
    'BPC_EXCHANGE_RATES': '/s_shared/v_actual_exchange_rate_bpc'
}

# Target data path
TARGET_PATH = '/finance/finance_table'

# Log file path
LOG_PATH = '/logs/finance_processing.log'

# Cluster configuration
CLUSTER_CONFIG = {
    'name': 'Finance Processing Cluster',
    'spark_version': '7.3.x-scala2.12',
    'node_type_id': 'Standard_DS3_v2',
    'driver_node_type_id': 'Standard_DS3_v2',
    'autoscale': {
        'min_workers': 2,
        'max_workers': 5
    },
    'autotermination_minutes': 30
}

# Special currency handling
SPECIAL_CURRENCY_RULES = {
    'local_currencies': ['EUR', 'USD', 'GBP'],
    'transaction_currencies': ['EUR', 'USD', 'JPY']
}