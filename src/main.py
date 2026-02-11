"""
Main module for the BNCPLS IF23B 10K File Generation process.
"""
import logging
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField

from config import get_spark_session, get_config
from aura_parser import parse_aura_payload
from field_calculator import calculate_10k_fields
from file_generator import calculate_filename, write_10k_files

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def extract_source_data(spark: SparkSession, src_sql: str) -> DataFrame:
    """
    Extract data from Oracle source tables using the provided SQL.
    
    Args:
        spark (SparkSession): Active SparkSession
        src_sql (str): SQL query to extract data
        
    Returns:
        DataFrame: Source data
    """
    logging.info("Extracting source data")
    
    # In a real environment, this would connect to Oracle
    # For this example, we'll simulate the source data
    
    # Create a schema that matches what we expect from the Oracle tables
    schema = StructType([
        StructField("ID", StringType(), False),
        StructField("POLICY_NUMBER", StringType(), False),
        StructField("BENEFICIARY_GENDER", StringType(), True),
        StructField("BENEFICIARY_FIRST_NAME", StringType(), True),
        StructField("BENEFICIARY_LAST_NAME", StringType(), True),
        StructField("APPLICATION_DATE", StringType(), True),
        StructField("I_AURA_INPUT_BASE64", StringType(), True)
    ])
    
    # Log the SQL that would be executed
    logging.info(f"Would execute SQL: {src_sql}")
    
    # Create a sample DataFrame that simulates what we'd get from Oracle
    # In a real environment, use:
    # df = spark.read.format("jdbc") \
    #     .option("url", oracle_url) \
    #     .option("query", src_sql) \
    #     .option("user", oracle_user) \
    #     .option("password", oracle_password) \
    #     .option("driver", "oracle.jdbc.driver.OracleDriver") \
    #     .load()
    
    # For this example, create sample data
    sample_data = [
        ("ID001", "POL0000001", "M", "JOHN", "DOE", "2023-01-15", "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz48QXVyYT48RGF0YT5TYW1wbGUgQXVyYSBEYXRhPC9EYXRhPjwvQXVyYT4="),
        ("ID002", "POL0000002", "F", "JANE", "SMITH", "2023-02-20", "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz48QXVyYT48VFhMaWZlPjxEYXRhPlNhbXBsZSBBdXJhIDE1IERhdGE8L0RhdGE+PC9UWExpZmU+PC9BdXJhPg=="),
        ("ID003", "POL0000003", "M", "ROBERT", "JOHNSON", "2023-03-10", None),
        ("ID004", "POL0000004", "F", "SUSAN", "WILLIAMS", "2023-04-05", "eyJpbnRlcnZpZXdEZXRhaWxzIjoiUEQ5NGJXd2dkbVZ5YzJsdmJqMGlNUzR3SWlCbGJtTnZaR2x1WnowaVZWUkdMVGdpUHo0OFFYVnlZVDQ4UkdGMFlUNVRZVzF3YkdVZ1FYVnlZU0JLVTBPRElFUmhkR0U4TDBSaGRHRStQQzlCZFhKaFBnPT0ifQ==")
    ]
    
    df = spark.createDataFrame(sample_data, schema)
    logging.info(f"Extracted {df.count()} records from source")
    
    return df

def process_aura_payloads(df: DataFrame, config: dict) -> DataFrame:
    """
    Process AURA payloads using the parse_aura_payload function.
    
    Args:
        df (DataFrame): Source data with I_AURA_INPUT_BASE64 column
        config (dict): Configuration parameters
        
    Returns:
        DataFrame: DataFrame with parsed AURA output
    """
    logging.info("Processing AURA payloads")
    
    # Register the UDF
    parse_aura_udf = udf(
        lambda i_aura_input_base64, policy_number: parse_aura_payload(
            i_aura_input_base64, 
            policy_number, 
            config['xslt_file'], 
            config['aura15_xslt_file']
        ),
        StringType()
    )
    
    # Apply the UDF to process AURA payloads
    result_df = df.withColumn(
        "PARSED_AURA_OUTPUT",
        parse_aura_udf(col("I_AURA_INPUT_BASE64"), col("POLICY_NUMBER"))
    )
    
    return result_df

def main():
    """Main processing function."""
    logging.info("Starting BNCPLS IF23B 10K File Generation")
    
    # Get configuration
    config = get_config()
    
    # Create SparkSession
    spark = get_spark_session()
    
    try:
        # Extract source data
        source_df = extract_source_data(spark, config['src_sql'])
        
        # Process AURA payloads
        df_with_aura = process_aura_payloads(source_df, config)
        
        # Calculate 10K fields (mapplet functionality)
        df_with_fields = calculate_10k_fields(df_with_aura)
        
        # Calculate file names
        df_with_filename = calculate_filename(df_with_fields, config)
        
        # Write 10K files
        write_10k_files(df_with_filename, config)
        
        logging.info("BNCPLS IF23B 10K File Generation completed successfully")
        
    except Exception as e:
        logging.error(f"Error in BNCPLS IF23B 10K File Generation: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()