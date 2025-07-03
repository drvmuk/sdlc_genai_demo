"""
Configuration settings for the customer order pipeline
"""

# Source data paths
CUSTOMER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Target table paths (catalog.schema.table format)
CUSTOMER_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customer"
ORDER_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.order"
ORDER_SUMMARY_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"

# SCD Type 2 configuration
SCD_COLUMNS = {
    "effective_start_date": "effective_start_date",
    "effective_end_date": "effective_end_date",
    "is_current": "is_current",
    "surrogate_key": "order_summary_sk"
}

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5