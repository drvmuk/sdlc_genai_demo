"""
Main module for the BNCPLS IF23B 10K File Generation pipeline.
"""
import argparse
import logging
import sys
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, struct

from src.config import PipelineConfig
from src.aura_parser import parse_aura_payload
from src.field_calculator import apply_field_calculations
from src.file_splitter import split_dataframe_by_row_count

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def parse_arguments() -> PipelineConfig:
    """
    Parse command line arguments.
    
    Returns:
        PipelineConfig object with parsed arguments
    """
    parser = argparse.ArgumentParser(description='BNCPLS IF23B 10K File Generator')
    
    parser.add_argument('--src_sql', required=True, help='SQL query for source data')
    parser.add_argument('--xslt_file', required=True, help='Path to XSLT file for Aura 14')
    parser.add_argument('--aura15_xslt_file', required=True, help='Path to XSLT file for Aura 15')
    parser.add_argument('--output_dir', required=True, help='Output directory for files')
    parser.add_argument('--file_prefix', default='IF23B', help='File prefix')
    parser.add_argument('--file_ext', default='dat', help='File extension')
    parser.add_argument('--batch_id', required=True, help='Batch ID')
    parser.add_argument('--run_id', help='Run ID')
    parser.add_argument('--max_rows_per_file', type=int, default=10000, 
                        help='Maximum rows per file')
    parser.add_argument('--log_level', default='INFO', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                        help='Logging level')
    
    args = parser.parse_args()
    
    return PipelineConfig(
        src_sql=args.src_sql,
        xslt_file=args.xslt_file,
        aura15_xslt_file=args.aura15_xslt_file,
        output_dir=args.output_dir,
        file_prefix=args.file_prefix,
        file_ext=args.file_ext,
        batch_id=args.batch_id,
        run_id=args.run_id,
        max_rows_per_file=args.max_rows_per_file,
        log_level=args.log_level
    )


def create_spark_session() -> SparkSession:
    """
    Create and configure a Spark session.
    
    Returns:
        Configured SparkSession
    """
    return (SparkSession.builder
            .appName("BNCPLS_IF23B_10K_File_Generator")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
            .config("spark.sql.files.openCostInBytes", "134217728")  # 128 MB
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.default.parallelism", "200")
            .getOrCreate())


def extract_source_data(spark: SparkSession, src_sql: str) -> DataFrame:
    """
    Extract data from source Oracle tables.
    
    Args:
        spark: SparkSession
        src_sql: SQL query for extraction
        
    Returns:
        DataFrame with source data
    """
    logger.info("Extracting source data")
    
    # Read from Oracle using JDBC
    # In a real implementation, connection details would be securely managed
    source_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:@//oracle-host:1521/service_name") \
        .option("dbtable", f"({src_sql}) src_data") \
        .option("user", "ZSYSBNCPLSDEV") \
        .option("password", "********") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()
    
    logger.info(f"Extracted {source_df.count()} records from source")
    return source_df


def process_aura_payloads(df: DataFrame, config: PipelineConfig) -> DataFrame:
    """
    Process AURA payloads in the DataFrame.
    
    Args:
        df: Input DataFrame with AURA payloads
        config: Pipeline configuration
        
    Returns:
        DataFrame with processed AURA payloads
    """
    logger.info("Processing AURA payloads")
    
    # Register the UDF for AURA parsing
    # Note: In a real implementation, we would use a proper UDF registration
    # This is a simplified approach for demonstration
    
    # Check if I_AURA_INPUT_BASE64 column exists
    if "I_AURA_INPUT_BASE64" in df.columns:
        # Apply the AURA parsing function
        result_df = df.withColumn(
            "parsed_aura_result",
            expr("parse_aura_payload(I_AURA_INPUT_BASE64, POLICY_NUMBER, " +
                 f"'{config.xslt_file}', '{config.aura15_xslt_file}')")
        )
        
        # Extract the parsed output and policy number
        result_df = result_df.withColumn(
            "PARSED_AURA_OUTPUT", 
            col("parsed_aura_result").getItem(0)
        ).drop("parsed_aura_result")
        
        logger.info("AURA payloads processed")
        return result_df
    else:
        logger.info("No AURA payloads to process (I_AURA_INPUT_BASE64 column not found)")
        return df


def run_pipeline(config: PipelineConfig) -> Dict[str, Any]:
    """
    Run the complete 10K file generation pipeline.
    
    Args:
        config: Pipeline configuration
        
    Returns:
        Dictionary with pipeline results
    """
    logger.info("Starting 10K file generation pipeline")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Extract source data
        source_df = extract_source_data(spark, config.src_sql)
        
        # Process AURA payloads if applicable
        df_with_aura = process_aura_payloads(source_df, config)
        
        # Apply field calculations (mapplet logic)
        df_with_fields = apply_field_calculations(df_with_aura)
        
        # Split into 10K files and write output
        file_info = split_dataframe_by_row_count(
            spark,
            df_with_fields,
            config.output_dir,
            config.file_prefix,
            config.batch_id,
            config.file_ext,
            config.max_rows_per_file
        )
        
        # Summarize results
        total_records = sum(info["row_count"] for info in file_info)
        total_files = len(file_info)
        
        result = {
            "status": "SUCCESS",
            "total_records": total_records,
            "total_files": total_files,
            "file_info": file_info
        }
        
        logger.info(f"Pipeline completed successfully: {total_records} records in {total_files} files")
        return result
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        return {
            "status": "FAILED",
            "error": str(e)
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    # Parse arguments
    config = parse_arguments()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, config.log_level))
    
    # Run pipeline
    result = run_pipeline(config)
    
    # Exit with appropriate code
    sys.exit(0 if result["status"] == "SUCCESS" else 1)