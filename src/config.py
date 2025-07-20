"""
Configuration settings for the Order Processing System.
"""

# Source data paths
CUSTOMER_CSV_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_CSV_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Delta table paths
CUSTOMER_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customer"
ORDER_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.order"
ORDER_SUMMARY_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"
CUSTOMER_AGGREGATE_SPEND_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend"

# SCD Type 2 columns
EFFECTIVE_FROM_COL = "effective_from"
EFFECTIVE_TO_COL = "effective_to"
IS_CURRENT_COL = "is_current"
INFINITE_DATE = "9999-12-31"  # End date for current records