"""
Custom DQX rules implementation for data quality validation in Databricks.
"""

import logging
import yaml
from typing import List, Dict, Any, Union, Optional
from pyspark.sql import DataFrame
import traceback

try:
    # Import DQX libraries
    from dqx import rule, make_condition
    from dqx.engine import DQEngine
    from databricks.sdk import WorkspaceClient
except ImportError:
    logging.error("Required DQX libraries not found. Please install DQX v0.7.0.")
    raise

class DQXCustomRules:
    """
    Implementation of custom DQX rules for data quality validation.
    """
    
    def __init__(self, workspace_client: Optional[WorkspaceClient] = None):
        """
        Initialize the DQXCustomRules class.
        
        Args:
            workspace_client: Databricks workspace client for DQX engine
        """
        self.logger = logging.getLogger(__name__)
        self.workspace_client = workspace_client or WorkspaceClient()
        self.dq_engine = None
        
    def register_rules(self):
        """
        Register all custom DQX rules.
        """
        try:
            self.logger.info("Registering custom DQX rules")
            # Rules are registered via decorators when the module is imported
            self.logger.info("Custom DQX rules registered successfully")
        except Exception as e:
            self.logger.error(f"Error registering custom DQX rules: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise
    
    def apply_dq_checks(self, df: DataFrame, rule_config: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Apply DQ checks to the input DataFrame.
        
        Args:
            df: Input Spark DataFrame to validate
            rule_config: Optional configuration for DQ rules
            
        Returns:
            DataFrame with DQ validation results
        """
        try:
            self.logger.info("Initializing DQ Engine")
            self.dq_engine = DQEngine(self.workspace_client)
            
            # Use default configuration if none provided
            if rule_config is None:
                rule_config = self._get_default_rule_config()
            
            self.logger.info("Applying DQ checks to input DataFrame")
            result_df = self.dq_engine.apply_checks(
                df=df,
                checks=rule_config,
                globals_dict=globals()
            )
            
            self.logger.info("DQ checks applied successfully")
            return result_df
        
        except Exception as e:
            self.logger.error(f"Error applying DQ checks: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise
    
    def _get_default_rule_config(self) -> Dict[str, Any]:
        """
        Get default rule configuration.
        
        Returns:
            Default rule configuration as dictionary
        """
        default_config = {
            "rules": {
                "null_check": {
                    "for_each_column": ["*"],
                    "args": {
                        "column_name": "${column}"
                    }
                },
                "primary_key_check": {
                    "args": {
                        "key_columns": ["id"],
                        "table_name": "example_table"
                    }
                }
            }
        }
        return default_config

    def load_rule_config_from_yaml(self, yaml_path: str) -> Dict[str, Any]:
        """
        Load rule configuration from YAML file.
        
        Args:
            yaml_path: Path to YAML configuration file
            
        Returns:
            Rule configuration as dictionary
        """
        try:
            with open(yaml_path, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except Exception as e:
            self.logger.error(f"Error loading YAML configuration: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise


# Define custom DQX rules using decorators

@rule(name="null_check", description="Check for null values in specified column")
def dqx_null_check(column_name: str):
    """
    Custom rule to check for null values in a specified column.
    
    Args:
        column_name: Name of the column to check for null values
        
    Returns:
        Condition expression for null value validation
    """
    try:
        # Return condition that evaluates to True for null values
        return make_condition(
            condition=f"col('{column_name}').isNull()",
            message=f"Column {column_name} contains null values",
            column=column_name
        )
    except Exception as e:
        logging.error(f"Error in null_check rule: {str(e)}")
        raise


@rule(name="primary_key_check", description="Validate primary key uniqueness")
def dqx_primary_key_check(key_columns: List[str], table_name: str):
    """
    Custom rule to validate primary key uniqueness.
    
    Args:
        key_columns: List of column names that form the primary key
        table_name: Name of the table for reference in error messages
        
    Returns:
        Condition expression for primary key validation
    """
    try:
        # Create a condition string that checks for duplicates in key columns
        key_cols_str = ", ".join([f"col('{col}')" for col in key_columns])
        key_cols_display = ", ".join(key_columns)
        
        return make_condition(
            condition=f"count() over (partition by {key_cols_str}) > 1",
            message=f"Duplicate primary key ({key_cols_display}) found in table {table_name}",
            columns=key_columns
        )
    except Exception as e:
        logging.error(f"Error in primary_key_check rule: {str(e)}")
        raise