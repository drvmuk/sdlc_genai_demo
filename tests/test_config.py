"""
Tests for configuration utilities.
"""

import pytest
import os
import tempfile
import yaml
from unittest.mock import patch, mock_open
import sys

# Add src directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from custom_dqx_rules.config import load_yaml_config, get_sample_config, save_config_to_yaml

def test_get_sample_config():
    """
    Test getting sample configuration.
    """
    # Act
    config = get_sample_config()
    
    # Assert
    assert "rules" in config
    assert "null_check" in config["rules"]
    assert "primary_key_check" in config["rules"]
    assert config["rules"]["null_check"]["for_each_column"] == ["id", "name", "email"]
    assert config["rules"]["primary_key_check"]["args"]["key_columns"] == ["id"]
    assert config["rules"]["primary_key_check"]["args"]["table_name"] == "customers"

def test_load_yaml_config():
    """
    Test loading YAML configuration.
    """
    # Arrange
    test_config = {
        "rules": {
            "test_rule": {
                "args": {
                    "param1": "value1"
                }
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        yaml.dump(test_config, temp_file)
        temp_file_path = temp_file.name
    
    try:
        # Act
        loaded_config = load_yaml_config(temp_file_path)
        
        # Assert
        assert loaded_config == test_config
        assert "rules" in loaded_config
        assert "test_rule" in loaded_config["rules"]
        assert loaded_config["rules"]["test_rule"]["args"]["param1"] == "value1"
    
    finally:
        # Clean up
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)

def test_load_yaml_config_error():
    """
    Test error handling when loading invalid YAML configuration.
    """
    # Arrange
    non_existent_file = "/path/to/nonexistent/file.yaml"
    
    # Act & Assert
    with pytest.raises(Exception):
        load_yaml_config(non_existent_file)

def test_save_config_to_yaml():
    """
    Test saving configuration to YAML file.
    """
    # Arrange
    test_config = {
        "rules": {
            "test_rule": {
                "args": {
                    "param1": "value1"
                }
            }
        }
    }
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = os.path.join(temp_dir, "config.yaml")
        
        # Act
        save_config_to_yaml(test_config, output_path)
        
        # Assert
        assert os.path.exists(output_path)
        
        with open(output_path, 'r') as file:
            loaded_config = yaml.safe_load(file)
            assert loaded_config == test_config