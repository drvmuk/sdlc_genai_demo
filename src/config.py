"""
Configuration module for the data processing pipeline.
"""

# Data paths
CUSTOMER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
ORDER_SUMMARY_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/ordersummary"
CUSTOMER_AGGREGATE_SPEND_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customeraggregatespend"

# Intermediate data paths
CLEANSED_CUSTOMER_DATA_PATH = "/tmp/cleansed_customer_data"
CLEANSED_ORDER_DATA_PATH = "/tmp/cleansed_order_data"
PROCESSED_DATA_PATH = "/tmp/processed_data"
AGGREGATED_DATA_PATH = "/tmp/aggregated_data"

# Mandatory columns
CUSTOMER_MANDATORY_COLUMNS = ["CustId", "Name", "Address"]
ORDER_MANDATORY_COLUMNS = ["OrderId", "CustId", "PricePerUnit", "Qty", "Date"]

# SCD Type 2 configuration
EFFECTIVE_FROM_COL = "EffectiveFrom"
EFFECTIVE_TO_COL = "EffectiveTo"
IS_CURRENT_COL = "IsCurrent"