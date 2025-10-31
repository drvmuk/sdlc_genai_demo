"""
Configuration settings for the customer order processing pipeline.
"""

# Volume paths
CUSTOMER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Catalog and schema information
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

# Table names
CUSTOMER_TABLE = "customer"
ORDER_TABLE = "order"
ORDER_SUMMARY_TABLE = "ordersummary"
CUSTOMER_AGGREGATE_SPEND_TABLE = "customeraggregatespend"