"""
Configuration settings for the DLT pipeline.
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
CUSTOMER_AGGREGATE_SPEND_TABLE = "customeraggregatespend"

# Log configuration
LOG_PATH = "/tmp/dlt_pipeline.log"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds