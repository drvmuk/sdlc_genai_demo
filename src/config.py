"""
Configuration settings for the Delta data processing pipeline
"""

# Source data paths
CUSTOMER_CSV_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_CSV_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Target Delta table paths
CUSTOMER_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customer"
ORDER_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.order"
ORDER_SUMMARY_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"
CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend"

# Schema definitions
ORDER_SUMMARY_SCHEMA = """
    CustId STRING,
    Name STRING,
    Address STRING,
    Phone STRING,
    OrderId STRING,
    Date DATE,
    TotalAmount DOUBLE,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN
"""

CUSTOMER_AGGREGATE_SPEND_SCHEMA = """
    Name STRING,
    Date DATE,
    TotalSpend DOUBLE
"""