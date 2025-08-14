"""
Configuration module for Finance Data Processor.

Contains configuration settings for the Finance Data Processing job.
"""

from typing import Dict

def get_config() -> Dict:
    """
    Get configuration settings for the Finance Data Processing job.
    
    Returns:
        Dictionary containing configuration settings
    """
    return {
        # Source data paths
        "faglflexa_path": "/mnt/data/ecc_everest/faglflexa",
        "bseg_path": "/mnt/data/ecc_everest/bseg",
        "entity_golden_view_path": "/mnt/data/golden_views/entity",
        "gl_golden_view_path": "/mnt/data/golden_views/gl",
        "trading_partner_golden_view_path": "/mnt/data/golden_views/trading_partner",
        "exchange_rates_path": "/mnt/data/bpc/s_shared.v_actual_exchange_rate_bpc",
        
        # Output path
        "output_path": "/mnt/data/finance/output",
        
        # Notification settings
        "admin_email": "finance.admin@company.com",
        
        # Job settings
        "retry_count": 3,
        "log_level": "INFO"
    }