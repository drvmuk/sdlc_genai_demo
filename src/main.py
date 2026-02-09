"""
Main workflow orchestration for E2E Address Change YUYU Creation.
Replaces the Informatica workflow wf_E2E_AC_TXDBH_DP_YUYU_CREATION.
"""
import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession

from src.source_count_check import check_source_records
from src.yuyu_creation import process_yuyu_records
from src.utils import create_trigger_file, run_cleanup_script

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure a Spark session."""
    return (SparkSession.builder
            .appName("E2E_AC_TXDBH_DP_YUYU_CREATION")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .getOrCreate())

def main():
    """Main workflow execution."""
    try:
        logger.info("Starting E2E Address Change YUYU Creation workflow")
        
        # Initialize Spark session
        spark = create_spark_session()
        
        # Get configuration from environment variables
        db_connection_stg = os.environ.get("DB_CONNECTION_E2E_ORA_STG")
        db_connection_ods = os.environ.get("DB_CONNECTION_E2E_ORA_ODS")
        target_file_dir = os.environ.get("TARGET_FILE_DIR")
        output_file_yuyu = os.environ.get("OUTPUT_FILE_YUYU_NEW")
        shell_dir = os.environ.get("SHELL_DIR")
        
        # Generate file timestamp for this run
        file_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Step 1: Check if there are source records to process
        record_count = check_source_records(
            spark, 
            db_connection_stg, 
            target_file_dir
        )
        
        logger.info(f"Source record count: {record_count}")
        
        # Step 2: Process based on record count
        if record_count == 0:
            # No records to process - create empty trigger file and run cleanup
            logger.info("No source records to process")
            create_trigger_file(target_file_dir, "DPACYUYU_TriggerFile.txt", "")
            run_cleanup_script(shell_dir, "E2E_AC_DelFile_DPAddressChangeYUYU.sh")
        else:
            # Records exist - process them and create output file
            logger.info(f"Processing {record_count} source records")
            
            # Step 3: Generate YUYU file and update staging table
            process_yuyu_records(
                spark,
                db_connection_stg,
                db_connection_ods,
                output_file_yuyu,
                target_file_dir,
                file_timestamp
            )
            
            # Step 4: Create trigger file with content and run cleanup
            create_trigger_file(target_file_dir, "DPACYUYU_TriggerFile.txt", "1")
            run_cleanup_script(shell_dir, "E2E_AC_DelFile_DPAddressChangeYUYU.sh")
        
        logger.info("E2E Address Change YUYU Creation workflow completed successfully")
        
    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()