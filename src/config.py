"""
Configuration settings for the ETL pipeline
"""

# Source data paths
CUSTOMER_DATA_PATH = "Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_DATA_PATH = "Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Target Delta tables
CUSTOMER_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.customer"
ORDER_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.order"
ORDER_SUMMARY_TABLE = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"

# Schema definitions
CUSTOMER_SCHEMA = "CustId INT, Name STRING, EmailId STRING, Region STRING"
ORDER_SCHEMA = "OrderId INT, ItemName STRING, PricePerUnit DOUBLE, Qty INT, Date DATE, CustId INT"