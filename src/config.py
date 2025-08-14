"""
Configuration settings for the Finance Data Processor
"""

# Source table configurations
SOURCE_TABLES = {
    "FAGLFLEXA": "ecc_everest.FAGLFLEXA",
    "BSEG": "ecc_everest.BSEG",
    "ENTITY_VIEW": "golden_views.entity",
    "GL_VIEW": "golden_views.gl",
    "TRADING_PARTNER_VIEW": "golden_views.trading_partner",
    "EXCHANGE_RATE": "s_shared.v_actual_exchange_rate_bpc",
    "REVENUE_ENTITY": "s_shared.v_revenue_entity_bpc"
}

# Target table configuration
TARGET_TABLE = "finance_data_mart.finance"

# Email configuration for error notifications
EMAIL_CONFIG = {
    "sender": "finance-data-alerts@example.com",
    "recipients": ["finance-team@example.com", "data-engineering@example.com"],
    "smtp_server": "smtp.example.com"
}

# List of local currencies for gain/loss calculation
LOCAL_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]

# List of transactional currencies for gain/loss calculation
TRANSACTIONAL_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]

# Realized and unrealized GL account patterns
REALIZED_GL_ACCOUNTS = ["4*", "5*"]
UNREALIZED_GL_ACCOUNTS = ["6*", "7*"]

# Logging configuration
LOG_CONFIG = {
    "log_level": "INFO",
    "log_path": "/dbfs/logs/finance_data_processor/"
}