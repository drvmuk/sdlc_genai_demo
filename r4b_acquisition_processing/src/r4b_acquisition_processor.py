"""
R4B Acquisition Contract Data Processing
This module implements the PySpark job to process and store R4B acquisition contract data
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from datetime import datetime
import uuid
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class R4BAcquisitionProcessor:
    """
    Processes R4B acquisition contract data by extracting from source tables, 
    performing transformations, and implementing watermarking logic.
    """
    
    def __init__(self, spark):
        """
        Initialize the processor with a SparkSession
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.execution_id = str(uuid.uuid4())
        self.execution_timestamp = datetime.now()
        self.source_paths = {
            "acquisition": "/mnt/data/r4b/staging",
            "contract": "/mnt/data/contracts",
            "control": "/mnt/data/control"
        }
        self.target_schema = "drvd__app_r4b"
        self.target_table = "r4b_acq_contract"
        
    def create_schema_and_table(self):
        """
        Create the target schema and table if they don't exist
        """
        logger.info("Stage 1: Creating schema and table if they don't exist")
        
        # Create schema if not exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.target_schema}")
        logger.info(f"Schema {self.target_schema} created or already exists")
        
        # Define table schema
        table_schema = """
        (
            LINE_KEY STRING,
            INIT_ACTIVATION_DATE TIMESTAMP,
            END_DATE TIMESTAMP,
            CONTRACT_ID STRING,
            PRODUCT_TYPE STRING,
            EFFECTIVE_DATE TIMESTAMP,
            COMMIT_END_DATE TIMESTAMP,
            COMMITMENT_TERM_MONTHS INT,
            COMMITMENT_AMOUNT DOUBLE,
            COMMITMENT_TYPE STRING,
            SOURCE_SYSTEM STRING,
            PROCESS_TYPE STRING,
            DATA_CHECKSUM STRING,
            CREATED_TIMESTAMP TIMESTAMP,
            CREATED_BY STRING,
            UPDATED_TIMESTAMP TIMESTAMP,
            UPDATED_BY STRING,
            EXECUTION_ID STRING
        )
        """
        
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.target_schema}.{self.target_table}
        {table_schema}
        USING DELTA
        PARTITIONED BY (INIT_ACTIVATION_DATE)
        COMMENT 'Processed R4B acquisition contract data with metadata'
        """
        
        self.spark.sql(create_table_sql)
        logger.info(f"Table {self.target_schema}.{self.target_table} created or already exists")
        
        # Validate table schema
        table_exists = self.spark.catalog.tableExists(self.target_schema, self.target_table)
        if table_exists:
            logger.info(f"Successfully validated that table {self.target_schema}.{self.target_table} exists")
        else:
            logger.error(f"Failed to create table {self.target_schema}.{self.target_table}")
            raise Exception(f"Failed to create target table {self.target_schema}.{self.target_table}")
    
    def extract_data(self):
        """
        Extract data from source tables
        
        Returns:
            tuple: (acquisition_df, contract_df, control_df)
        """
        logger.info("Stage 2: Extracting data from source tables")
        
        try:
            # Load acquisition data
            acquisition_df = self.spark.read.format("delta").load(f"{self.source_paths['acquisition']}")
            acq_count = acquisition_df.count()
            logger.info(f"Loaded {acq_count} records from R4B_SUB_ACQUISITION_FACT_STG")
            
            # Load contract data
            contract_df = self.spark.read.format("delta").load(f"{self.source_paths['contract']}")
            contract_count = contract_df.count()
            logger.info(f"Loaded {contract_count} records from CONTRACT")
            
            # Load control data
            control_df = self.spark.read.format("delta").load(f"{self.source_paths['control']}")
            logger.info(f"Loaded control parameters from margin_control")
            
            return acquisition_df, contract_df, control_df
            
        except Exception as e:
            logger.error(f"Error extracting source data: {str(e)}")
            raise
    
    def process_flow_1(self, acquisition_df, contract_df):
        """
        Process Flow 1: END_DATE based join and transformation
        
        Args:
            acquisition_df: R4B subscriber acquisition fact DataFrame
            contract_df: Contract information DataFrame
            
        Returns:
            DataFrame: Processed Flow 1 results
        """
        logger.info("Stage 3: Processing Flow 1 (END_DATE based join)")
        
        try:
            # Apply filters to contract data
            filtered_contract = contract_df.filter(
                (F.col("PRODUCT_TYPE").isin("C", "A")) &
                (F.col("GG_OP_TYPE") != "D")
            )
            
            # Join acquisition and contract data based on END_DATE
            flow1_df = acquisition_df.join(
                filtered_contract,
                on="CONTRACT_ID",
                how="inner"
            ).filter(
                (F.col("EFFECTIVE_DATE") <= F.col("END_DATE")) &
                ((F.col("COMMIT_END_DATE") > F.col("END_DATE")) | F.col("COMMIT_END_DATE").isNull())
            )
            
            flow1_count = flow1_df.count()
            logger.info(f"Flow 1 processing completed with {flow1_count} records")
            
            # Add process type identifier
            flow1_df = flow1_df.withColumn("PROCESS_TYPE", F.lit("FLOW_1"))
            
            return flow1_df
            
        except Exception as e:
            logger.error(f"Error in Flow 1 processing: {str(e)}")
            raise
    
    def process_flow_2(self, acquisition_df, contract_df):
        """
        Process Flow 2: INIT_ACTIVATION_DATE based join and transformation
        
        Args:
            acquisition_df: R4B subscriber acquisition fact DataFrame
            contract_df: Contract information DataFrame
            
        Returns:
            DataFrame: Processed Flow 2 results
        """
        logger.info("Stage 4: Processing Flow 2 (INIT_ACTIVATION_DATE based join)")
        
        try:
            # Apply filters to contract data
            filtered_contract = contract_df.filter(
                (F.col("PRODUCT_TYPE").isin("C", "A")) &
                (F.col("GG_OP_TYPE") != "D")
            )
            
            # Join acquisition and contract data based on INIT_ACTIVATION_DATE
            flow2_df = acquisition_df.join(
                filtered_contract,
                on="CONTRACT_ID",
                how="inner"
            ).filter(
                (F.col("EFFECTIVE_DATE") <= F.col("INIT_ACTIVATION_DATE")) &
                ((F.col("COMMIT_END_DATE") > F.col("INIT_ACTIVATION_DATE")) | F.col("COMMIT_END_DATE").isNull())
            )
            
            flow2_count = flow2_df.count()
            logger.info(f"Flow 2 processing completed with {flow2_count} records")
            
            # Add process type identifier
            flow2_df = flow2_df.withColumn("PROCESS_TYPE", F.lit("FLOW_2"))
            
            return flow2_df
            
        except Exception as e:
            logger.error(f"Error in Flow 2 processing: {str(e)}")
            raise
    
    def join_and_transform(self, flow1_df, flow2_df):
        """
        Join Flow 1 and Flow 2 results
        
        Args:
            flow1_df: Results from Flow 1 processing
            flow2_df: Results from Flow 2 processing
            
        Returns:
            DataFrame: Combined and transformed results
        """
        logger.info("Stage 5: Joining Flow 1 and Flow 2 results")
        
        try:
            # Perform full outer join on LINE_KEY and INIT_ACTIVATION_DATE
            combined_df = flow1_df.select(
                "LINE_KEY", "INIT_ACTIVATION_DATE", "END_DATE", "CONTRACT_ID", 
                "PRODUCT_TYPE", "EFFECTIVE_DATE", "COMMIT_END_DATE", 
                "COMMITMENT_TERM_MONTHS", "COMMITMENT_AMOUNT", "COMMITMENT_TYPE",
                "SOURCE_SYSTEM", "PROCESS_TYPE"
            ).join(
                flow2_df.select(
                    "LINE_KEY", "INIT_ACTIVATION_DATE", "END_DATE", "CONTRACT_ID", 
                    "PRODUCT_TYPE", "EFFECTIVE_DATE", "COMMIT_END_DATE", 
                    "COMMITMENT_TERM_MONTHS", "COMMITMENT_AMOUNT", "COMMITMENT_TYPE",
                    "SOURCE_SYSTEM", "PROCESS_TYPE"
                ),
                on=["LINE_KEY", "INIT_ACTIVATION_DATE"],
                how="fullouter"
            )
            
            # Resolve conflicts between Flow 1 and Flow 2 results
            # Priority given to Flow 1 when both exist
            resolved_df = combined_df.withColumn(
                "PROCESS_TYPE",
                F.when(F.col("PROCESS_TYPE") == "FLOW_1", "FLOW_1")
                .otherwise(F.col("PROCESS_TYPE"))
            )
            
            combined_count = resolved_df.count()
            logger.info(f"Join and transform completed with {combined_count} records")
            
            return resolved_df
            
        except Exception as e:
            logger.error(f"Error in join and transform: {str(e)}")
            raise
    
    def apply_watermarking(self, combined_df, control_df):
        """
        Apply watermarking logic based on control parameters
        
        Args:
            combined_df: Combined results from Flow 1 and Flow 2
            control_df: Control table with processing parameters
            
        Returns:
            tuple: (final_df, watermark_start_date, watermark_end_date)
        """
        logger.info("Stage 6: Applying watermarking logic")
        
        try:
            # Extract watermark parameters from control table
            watermark_params = control_df.filter(F.col("control_name") == "r4b_acquisition_watermark").collect()
            
            if not watermark_params:
                logger.warning("No watermark parameters found, using default values")
                watermark_start_date = datetime.now().replace(day=1)  # First day of current month
                watermark_end_date = datetime.now()
            else:
                param_row = watermark_params[0]
                watermark_start_date = param_row.getAs("start_date")
                watermark_end_date = param_row.getAs("end_date")
            
            logger.info(f"Watermark date range: {watermark_start_date} to {watermark_end_date}")
            
            # Filter data based on watermark
            final_df = combined_df.filter(
                (F.col("INIT_ACTIVATION_DATE") >= F.lit(watermark_start_date)) &
                (F.col("INIT_ACTIVATION_DATE") <= F.lit(watermark_end_date))
            )
            
            watermark_count = final_df.count()
            logger.info(f"Watermarking applied, {watermark_count} records within date range")
            
            return final_df, watermark_start_date, watermark_end_date
            
        except Exception as e:
            logger.error(f"Error applying watermarking: {str(e)}")
            raise
    
    def delete_existing_records(self, watermark_start_date, watermark_end_date):
        """
        Delete existing records based on watermark
        
        Args:
            watermark_start_date: Start date for watermark range
            watermark_end_date: End date for watermark range
        """
        logger.info("Stage 7: Deleting existing records based on watermark")
        
        try:
            # Count records to be deleted
            count_query = f"""
            SELECT COUNT(*) as delete_count FROM {self.target_schema}.{self.target_table}
            WHERE INIT_ACTIVATION_DATE BETWEEN '{watermark_start_date}' AND '{watermark_end_date}'
            """
            
            delete_count = self.spark.sql(count_query).collect()[0]["delete_count"]
            logger.info(f"Found {delete_count} existing records to delete within watermark range")
            
            # Delete records
            delete_query = f"""
            DELETE FROM {self.target_schema}.{self.target_table}
            WHERE INIT_ACTIVATION_DATE BETWEEN '{watermark_start_date}' AND '{watermark_end_date}'
            """
            
            self.spark.sql(delete_query)
            logger.info(f"Successfully deleted {delete_count} records within watermark range")
            
        except Exception as e:
            logger.error(f"Error deleting existing records: {str(e)}")
            raise
    
    def add_metadata_and_write(self, final_df):
        """
        Add metadata columns and write final data to target table
        
        Args:
            final_df: Final DataFrame to write
        """
        logger.info("Stage 8: Adding metadata and writing final data")
        
        try:
            # Add metadata columns
            enriched_df = final_df.withColumn(
                "DATA_CHECKSUM", 
                F.md5(F.concat_ws("||", *[c for c in final_df.columns if c not in ["PROCESS_TYPE"]]))
            ).withColumn(
                "CREATED_TIMESTAMP", 
                F.lit(self.execution_timestamp)
            ).withColumn(
                "CREATED_BY", 
                F.lit("R4B_ACQUISITION_PROCESSOR")
            ).withColumn(
                "UPDATED_TIMESTAMP", 
                F.lit(self.execution_timestamp)
            ).withColumn(
                "UPDATED_BY", 
                F.lit("R4B_ACQUISITION_PROCESSOR")
            ).withColumn(
                "EXECUTION_ID", 
                F.lit(self.execution_id)
            )
            
            # Write to target table
            enriched_df.write.format("delta").mode("append").saveAsTable(f"{self.target_schema}.{self.target_table}")
            
            final_count = enriched_df.count()
            logger.info(f"Successfully wrote {final_count} records to {self.target_schema}.{self.target_table}")
            
        except Exception as e:
            logger.error(f"Error adding metadata and writing data: {str(e)}")
            raise
    
    def run(self):
        """
        Run the full R4B acquisition processing pipeline
        """
        start_time = time.time()
        logger.info(f"Starting R4B acquisition processing with execution ID: {self.execution_id}")
        
        try:
            # Stage 1: Create schema and table
            self.create_schema_and_table()
            
            # Stage 2: Extract data from source tables
            acquisition_df, contract_df, control_df = self.extract_data()
            
            # Stage 3: Process Flow 1
            flow1_df = self.process_flow_1(acquisition_df, contract_df)
            
            # Stage 4: Process Flow 2
            flow2_df = self.process_flow_2(acquisition_df, contract_df)
            
            # Stage 5: Join Flow 1 and Flow 2 results
            combined_df = self.join_and_transform(flow1_df, flow2_df)
            
            # Stage 6: Apply watermarking logic
            final_df, watermark_start_date, watermark_end_date = self.apply_watermarking(combined_df, control_df)
            
            # Stage 7: Delete existing records based on watermark
            self.delete_existing_records(watermark_start_date, watermark_end_date)
            
            # Stage 8: Add metadata and write final data
            self.add_metadata_and_write(final_df)
            
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f"R4B acquisition processing completed successfully in {execution_time:.2f} seconds")
            
            # Generate execution summary
            logger.info("=== Execution Summary ===")
            logger.info(f"Execution ID: {self.execution_id}")
            logger.info(f"Status: SUCCESS")
            logger.info(f"Records processed: {final_df.count()}")
            logger.info(f"Execution time: {execution_time:.2f} seconds")
            logger.info("=======================")
            
            return True
            
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            logger.error(f"R4B acquisition processing failed: {str(e)}")
            
            # Generate execution summary
            logger.info("=== Execution Summary ===")
            logger.info(f"Execution ID: {self.execution_id}")
            logger.info(f"Status: FAILED")
            logger.info(f"Error: {str(e)}")
            logger.info(f"Execution time: {execution_time:.2f} seconds")
            logger.info("=======================")
            
            raise

def main():
    """
    Main entry point for the R4B acquisition processing job
    """
    spark = SparkSession.builder \
        .appName("R4B Acquisition Contract Data Processing") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    processor = R4BAcquisitionProcessor(spark)
    processor.run()

if __name__ == "__main__":
    main()