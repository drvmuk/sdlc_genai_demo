"""
Configuration module for the BNCPLS IF23B 10K File Generation project.
"""
from pyspark.sql import SparkSession
import argparse
import os

def get_spark_session(app_name="BNCPLS_IF23B_10K_File_Generation"):
    """
    Create and return a SparkSession with appropriate configurations.
    
    Args:
        app_name (str): Name of the Spark application
        
    Returns:
        SparkSession: Configured SparkSession instance
    """
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .getOrCreate())

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='BNCPLS IF23B 10K File Generation')
    
    parser.add_argument('--src_sql', type=str, required=True,
                        help='SQL query to extract data from source tables')
    
    parser.add_argument('--xslt_file', type=str, required=True,
                        help='Path to XSLT file for Aura 14 payloads')
    
    parser.add_argument('--aura15_xslt_file', type=str, required=True,
                        help='Path to XSLT file for Aura 15 payloads')
    
    parser.add_argument('--output_dir', type=str, required=True,
                        help='Output directory for flat files')
    
    parser.add_argument('--batch_id', type=str, default='',
                        help='Batch ID for file naming')
    
    parser.add_argument('--run_id', type=str, default='',
                        help='Run ID for file naming')
    
    parser.add_argument('--file_prefix', type=str, default='IF23B',
                        help='Prefix for output file names')
    
    parser.add_argument('--file_ext', type=str, default='dat',
                        help='Extension for output files')
    
    parser.add_argument('--max_rows_per_file', type=int, default=10000,
                        help='Maximum number of rows per output file')
    
    return parser.parse_args()

def get_config():
    """
    Get configuration from command line arguments or environment variables.
    
    Returns:
        dict: Configuration parameters
    """
    # Try to get from command line first
    try:
        args = parse_arguments()
        config = {
            'src_sql': args.src_sql,
            'xslt_file': args.xslt_file,
            'aura15_xslt_file': args.aura15_xslt_file,
            'output_dir': args.output_dir,
            'batch_id': args.batch_id,
            'run_id': args.run_id,
            'file_prefix': args.file_prefix,
            'file_ext': args.file_ext,
            'max_rows_per_file': args.max_rows_per_file
        }
    except:
        # Fall back to environment variables
        config = {
            'src_sql': os.environ.get('SRC_SQL', ''),
            'xslt_file': os.environ.get('XSLT_FILE_PATH', ''),
            'aura15_xslt_file': os.environ.get('AURA15_XSLT_FILE_PATH', ''),
            'output_dir': os.environ.get('OUTPUT_DIR', '/tmp/output'),
            'batch_id': os.environ.get('BATCH_ID', ''),
            'run_id': os.environ.get('RUN_ID', ''),
            'file_prefix': os.environ.get('FILE_PREFIX', 'IF23B'),
            'file_ext': os.environ.get('FILE_EXT', 'dat'),
            'max_rows_per_file': int(os.environ.get('MAX_ROWS_PER_FILE', '10000'))
        }
    
    return config