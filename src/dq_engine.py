"""
Data Quality Engine implementation using DQX library.
"""
import yaml
from pyspark.sql import DataFrame
import logging
from typing import Dict, Any, Optional
import importlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Import DQX library
    from databricks.labs.dqx.engines import DQXEngine
except ImportError:
    logger.error("Failed to import DQX library. Make sure 'databricks-labs-dqx' is installed.")
    raise

class DQEngine:
    """
    Data Quality Engine class for applying custom DQX rules.
    """
    
    def __init__(self):
        """Initialize the DQ Engine with DQX."""
        try:
            self.dq_engine = DQXEngine()
            logger.info("DQX Engine initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize DQX Engine: {str(e)}")
            raise
    
    def parse_yaml_metadata(self, yaml_str: str) -> Dict[str, Any]:
        """
        Parse YAML metadata string into a dictionary.
        
        Args:
            yaml_str: YAML metadata string
            
        Returns:
            Dictionary representation of the YAML metadata
        """
        try:
            metadata = yaml.safe_load(yaml_str)
            logger.info("YAML metadata parsed successfully")
            return metadata
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML metadata: {str(e)}")
            raise
    
    def apply_checks(self, df: DataFrame, metadata: Dict[str, Any], custom_rules: Dict[str, Any]) -> DataFrame:
        """
        Apply data quality checks to the DataFrame based on metadata.
        
        Args:
            df: Input DataFrame to validate
            metadata: Dictionary containing check definitions
            custom_rules: Dictionary of custom rule functions
            
        Returns:
            DataFrame with validation results
        """
        try:
            logger.info("Applying data quality checks")
            
            # Apply checks using DQX engine
            result_df = self.dq_engine.apply_checks_by_metadata(
                df=df,
                metadata=metadata,
                custom_rules=custom_rules
            )
            
            logger.info("Data quality checks applied successfully")
            return result_df
        except Exception as e:
            logger.error(f"Error applying data quality checks: {str(e)}")
            raise

def validate_dataframe(df: DataFrame, yaml_metadata: str, output_path: Optional[str] = None) -> DataFrame:
    """
    Validate a DataFrame using custom DQX rules defined in YAML metadata.
    
    Args:
        df: Input DataFrame to validate
        yaml_metadata: YAML string containing check definitions
        output_path: Optional path to save the validated DataFrame
        
    Returns:
        DataFrame with validation results
    """
    try:
        # Import custom rules
        from dqx_custom_rules.dq_rules import dqx_null_check, dqx_primary_check
        
        # Create custom rules dictionary
        custom_rules = {
            'dqx_null_check': dqx_null_check,
            'dqx_primary_check': dqx_primary_check
        }
        
        # Initialize DQ Engine
        dq_engine = DQEngine()
        
        # Parse YAML metadata
        metadata = dq_engine.parse_yaml_metadata(yaml_metadata)
        
        # Apply checks
        result_df = dq_engine.apply_checks(df, metadata, custom_rules)
        
        # Save to output path if specified
        if output_path:
            result_df.write.format("delta").mode("overwrite").save(output_path)
            logger.info(f"Validation results saved to {output_path}")
        
        return result_df
    
    except Exception as e:
        logger.error(f"Error in validate_dataframe: {str(e)}")
        raise