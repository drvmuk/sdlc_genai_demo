"""
Configuration parameters for the data pipeline.
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

# Schema definitions
CUSTOMER_SCHEMA = ["CustId", "Name", "EmailId", "Region"]
ORDER_SCHEMA = ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
ORDER_SUMMARY_SCHEMA = ["CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", "PricePerUnit", "Qty", "Date", 
                        "TotalAmount", "IsActive", "StartDate", "EndDate"]
CUSTOMER_AGGREGATE_SCHEMA = ["Name", "TotalAmount", "Date"]