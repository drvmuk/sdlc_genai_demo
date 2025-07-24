"""
Example usage of the DQX custom rules.
"""
from pyspark.sql import SparkSession
from dqx_custom_rules.dq_engine import validate_dataframe
from data.sample_data import create_sample_data

def main():
    """
    Example usage of the DQX custom rules.
    """
    # Create Spark session
    spark = SparkSession.builder.appName("DQX Example").getOrCreate()
    
    # Create sample data
    df = create_sample_data(spark)
    
    # Define YAML metadata for checks
    yaml_metadata = """
    checks:
      - columns: [customer_id, name, email]
        rules: [dqx_null_check]
      - columns: [customer_id]
        rules: [dqx_primary_check]
      - columns: [email]
        rules: [dqx_primary_check]
    """
    
    # Validate the DataFrame
    validated_df = validate_dataframe(df, yaml_metadata, "/tmp/validated_data")
    
    # Show validation results
    print("Validation Results:")
    validated_df.show()
    
    # Extract and display failed records
    print("\nFailed Records:")
    failed_records = validated_df.filter("_dqx_failed == true")
    failed_records.show()
    
    # Display validation summary
    print("\nValidation Summary:")
    validation_summary = validated_df.groupBy("_dqx_failed").count()
    validation_summary.show()

if __name__ == "__main__":
    main()