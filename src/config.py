"""
Configuration settings for the Databricks Delta ETL pipeline.
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
CUSTOMER_AGGREGATE_TABLE = "customeraggregatespend"

# SCD Type 2 configuration
SCD_TRACKING_COLUMNS = ["Name", "Address", "Phone"]