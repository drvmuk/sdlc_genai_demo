"""Configuration settings for the data processing pipeline."""

# Volume paths
CUSTOMER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Catalog and schema settings
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

# Table names
CUSTOMER_TABLE = "customer"
ORDER_TABLE = "order"
ORDER_SUMMARY_TABLE = "ordersummary"
CUSTOMER_AGGREGATE_SPEND_TABLE = "customeraggregatespend"

# Schema definitions
CUSTOMER_SCHEMA = "CustId STRING, Name STRING, EmailId STRING, Region STRING"
ORDER_SCHEMA = "OrderId STRING, ItemName STRING, PricePerUnit DOUBLE, Qty INT, Date DATE, CustId STRING"
ORDER_SUMMARY_SCHEMA = """
    CustId STRING, 
    Name STRING, 
    EmailId STRING, 
    Region STRING, 
    OrderId STRING, 
    ItemName STRING, 
    PricePerUnit DOUBLE, 
    Qty INT, 
    Date DATE, 
    TotalAmount DOUBLE,
    IsActive BOOLEAN,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP
"""
CUSTOMER_AGGREGATE_SCHEMA = "Name STRING, TotalAmount DOUBLE, Date DATE"