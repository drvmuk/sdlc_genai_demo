"""
Configuration settings for the Databricks Delta Live Tables Pipeline.
"""

# Source data paths
CUSTOMER_CSV_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_CSV_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Target Delta table locations
CUSTOMER_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customer"
ORDER_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.order"
ORDERSUMMARY_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"
CUSTOMERAGGREGATESPEND_DELTA_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend"

# Logging configuration
LOG_LEVEL = "INFO"