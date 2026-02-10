"""
Main module for E2E Address Change YUYU processing.

This module provides the entry point for the address change processing job.
"""
from pyspark.sql import SparkSession
from address_change_processor import AddressChangeProcessor
import os
import argparse


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process address change records for YUYU output')
    parser.add_argument('--process-userid', required=True, help='User ID for process stamping')
    parser.add_argument('--source-table', default='ZSYSE2EDEV.STG_E2E_AC_TXDBH_DATA', 
                        help='Source staging table name')
    parser.add_argument('--target-table', default='ZSYSE2EDEV.STG_E2E_AC_TXDBH_DATA', 
                        help='Target staging table name')
    parser.add_argument('--output-path', default='/tmp/yuyu_output/DPAddressChangeYUYU', 
                        help='Output path for YUYU extract')
    return parser.parse_args()


def main():
    """Main entry point for the job."""
    args = parse_arguments()
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("E2E Address Change YUYU Processing") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Initialize processor
    processor = AddressChangeProcessor(spark, args.process_userid)
    
    # Execute processing
    processor.process(args.source_table, args.target_table, args.output_path)
    
    spark.stop()


if __name__ == "__main__":
    main()