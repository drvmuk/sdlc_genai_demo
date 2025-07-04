"""
Configuration settings for the SCD Type 2 data pipeline.
"""

# Source data paths
CUSTOMER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Target catalog and schema
TARGET_CATALOG = "gen_ai_poc_databrickscoe"
TARGET_SCHEMA = "sdlc_wizard"

# Table names
CUSTOMER_TABLE = "customer"
ORDER_TABLE = "order"
ORDER_SUMMARY_TABLE = "ordersummary"

# Full table paths
CUSTOMER_TABLE_PATH = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{CUSTOMER_TABLE}"
ORDER_TABLE_PATH = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{ORDER_TABLE}"
ORDER_SUMMARY_TABLE_PATH = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{ORDER_SUMMARY_TABLE}"

# SCD Type 2 configuration
EFFECTIVE_FROM_COL = "effective_from"
EFFECTIVE_TO_COL = "effective_to"
CURRENT_FLAG_COL = "is_current"
INFINITE_DATE = "9999-12-31"  # Date used to indicate a record is current