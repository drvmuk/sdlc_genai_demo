"""
Configuration utilities for DQX rules.
"""

import yaml
import logging
from typing import Dict, Any, List
import os

def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    Load YAML configuration file.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Configuration as dictionary
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"Error loading configuration from {config_path}: {str(e)}")
        raise

def get_sample_config() -> Dict[str, Any]:
    """
    Get sample DQX rule configuration.
    
    Returns:
        Sample configuration as dictionary
    """
    return {
        "rules": {
            "null_check": {
                "for_each_column": ["id", "name", "email"],
                "args": {
                    "column_name": "${column}"
                }
            },
            "primary_key_check": {
                "args": {
                    "key_columns": ["id"],
                    "table_name": "customers"
                }
            }
        }
    }

def save_config_to_yaml(config: Dict[str, Any], output_path: str) -> None:
    """
    Save configuration to YAML file.
    
    Args:
        config: Configuration dictionary
        output_path: Path to save YAML file
    """
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
        logging.info(f"Configuration saved to {output_path}")
    except Exception as e:
        logging.error(f"Error saving configuration to {output_path}: {str(e)}")
        raise