"""
Configuration settings for the ETL pipeline.
"""

# Data paths
CUSTOMER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Temporary paths for intermediate data
CLEANSED_CUSTOMER_DATA_PATH = "/tmp/cleansed_customer_data"
CLEANSED_ORDER_DATA_PATH = "/tmp/cleansed_order_data"
JOINED_DATA_PATH = "/tmp/joined_data"

# Database and table information
DATABASE_NAME = "gen_ai_poc_databrickscoe"
SCHEMA_NAME = "sdlc_wizard"
TABLE_NAME = "ordersummary"
FULL_TABLE_NAME = f"{DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

# Schema definitions
CUSTOMER_SCHEMA = {
    "CustId": "string",
    "FirstName": "string",
    "LastName": "string",
    "Email": "string",
    "Phone": "string",
    "Address": "string",
    "City": "string",
    "State": "string",
    "ZipCode": "string"
}

ORDER_SCHEMA = {
    "OrderId": "string",
    "CustId": "string",
    "OrderDate": "date",
    "ShipDate": "date",
    "OrderTotal": "double",
    "OrderStatus": "string"
}

ORDERSUMMARY_SCHEMA = """
    CustId STRING,
    FirstName STRING,
    LastName STRING,
    Email STRING,
    Phone STRING,
    Address STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    OrderId STRING,
    OrderDate DATE,
    ShipDate DATE,
    OrderTotal DOUBLE,
    OrderStatus STRING,
    IsActive BOOLEAN,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP
"""