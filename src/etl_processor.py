"""
ETL processor for customer and order data
Handles data ingestion, cleansing, and transformation according to SCD Type 2 logic
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when, coalesce, concat_ws, sha2
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from typing import Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("etl_processor")

class ETLProcessor:
    def __init__(self, spark: SparkSession):
        """
        Initialize ETL processor
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        
    def ingest_and_cleanse_data(self) -> Tuple[DataFrame, DataFrame]:
        """
        Ingest and cleanse customer and order data from CSV files
        
        Returns:
            Tuple containing cleansed customer and order DataFrames
        """
        try:
            logger.info("Starting data ingestion process")
            
            # Read customer data
            customer_data = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
                
            # Read order data
            order_data = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
                
            logger.info(f"Successfully read customer data with {customer_data.count()} rows")
            logger.info(f"Successfully read order data with {order_data.count()} rows")
            
            # Cleanse customer data - remove nulls and duplicates
            cleansed_customer_data = customer_data.dropna().dropDuplicates()
            
            # Cleanse order data - remove nulls and duplicates
            cleansed_order_data = order_data.dropna().dropDuplicates()
            
            logger.info(f"After cleansing: customer data has {cleansed_customer_data.count()} rows")
            logger.info(f"After cleansing: order data has {cleansed_order_data.count()} rows")
            
            # Save cleansed data to Delta
            cleansed_customer_data.write \
                .format("delta") \
                .mode("overwrite") \
                .save("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/cleansed_customer_data")
                
            cleansed_order_data.write \
                .format("delta") \
                .mode("overwrite") \
                .save("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/cleansed_order_data")
                
            logger.info("Successfully saved cleansed data to Delta format")
            
            return cleansed_customer_data, cleansed_order_data
            
        except Exception as e:
            logger.error(f"Error in data ingestion and cleansing: {str(e)}")
            raise
            
    def create_ordersummary_table(self) -> None:
        """
        Create the ordersummary table if it doesn't exist
        """
        try:
            logger.info("Checking if ordersummary table exists")
            
            # Define the schema for ordersummary table
            schema = StructType([
                StructField("CustId", StringType(), False),
                StructField("Name", StringType(), True),
                StructField("Address", StringType(), True),
                StructField("Phone", StringType(), True),
                StructField("OrderId", StringType(), True),
                StructField("OrderDate", TimestampType(), True),
                StructField("Amount", StringType(), True),
                StructField("EffectiveDate", TimestampType(), False),
                StructField("EndDate", TimestampType(), True),
                StructField("IsCurrent", BooleanType(), False)
            ])
            
            # Create the table if it doesn't exist
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary
                (
                    CustId STRING NOT NULL,
                    Name STRING,
                    Address STRING,
                    Phone STRING,
                    OrderId STRING,
                    OrderDate TIMESTAMP,
                    Amount STRING,
                    EffectiveDate TIMESTAMP NOT NULL,
                    EndDate TIMESTAMP,
                    IsCurrent BOOLEAN NOT NULL
                )
                USING DELTA
            """)
            
            logger.info("Successfully created or verified ordersummary table")
            
        except Exception as e:
            logger.error(f"Error creating ordersummary table: {str(e)}")
            raise
            
    def load_initial_ordersummary(self, customer_df: DataFrame, order_df: DataFrame) -> None:
        """
        Join cleansed customer and order data and load into ordersummary table
        
        Args:
            customer_df: Cleansed customer DataFrame
            order_df: Cleansed order DataFrame
        """
        try:
            logger.info("Starting initial load of ordersummary table")
            
            # Join customer and order data
            joined_data = customer_df.join(
                order_df,
                customer_df.CustId == order_df.CustId,
                "inner"
            ).select(
                customer_df.CustId,
                customer_df.Name,
                customer_df.Address,
                customer_df.Phone,
                order_df.OrderId,
                order_df.OrderDate,
                order_df.Amount
            )
            
            # Add SCD Type 2 columns
            current_time = current_timestamp()
            scd_data = joined_data \
                .withColumn("EffectiveDate", current_time) \
                .withColumn("EndDate", lit(None).cast("timestamp")) \
                .withColumn("IsCurrent", lit(True))
                
            # Write to ordersummary table
            scd_data.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
                
            logger.info(f"Successfully loaded {scd_data.count()} records into ordersummary table")
            
        except Exception as e:
            logger.error(f"Error in initial load of ordersummary: {str(e)}")
            raise
            
    def update_ordersummary_scd2(self, new_customer_df: DataFrame) -> None:
        """
        Update ordersummary table using SCD Type 2 logic
        
        Args:
            new_customer_df: New customer data for updates
        """
        try:
            logger.info("Starting SCD Type 2 update for ordersummary table")
            
            # Read current ordersummary data
            current_data = self.spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
            
            # Get only current records
            current_records = current_data.filter(col("IsCurrent") == True)
            
            # Create a hash of the customer attributes to detect changes
            current_records_with_hash = current_records.withColumn(
                "customer_hash",
                sha2(concat_ws("||", col("Name"), col("Address"), col("Phone")), 256)
            )
            
            new_customer_with_hash = new_customer_df.withColumn(
                "customer_hash",
                sha2(concat_ws("||", col("Name"), col("Address"), col("Phone")), 256)
            )
            
            # Join to find changed records
            joined = current_records_with_hash.join(
                new_customer_with_hash,
                current_records_with_hash.CustId == new_customer_with_hash.CustId,
                "inner"
            )
            
            # Identify changed records
            changed_records = joined.filter(
                joined["customer_hash"] != joined["customer_hash_1"]
            ).select(current_records_with_hash["*"])
            
            # Create expired records (old versions)
            current_time = current_timestamp()
            expired_records = changed_records \
                .withColumn("EndDate", current_time) \
                .withColumn("IsCurrent", lit(False))
                
            # Create new current records
            new_current_records = joined.filter(
                joined["customer_hash"] != joined["customer_hash_1"]
            ).select(
                new_customer_with_hash.CustId,
                new_customer_with_hash.Name,
                new_customer_with_hash.Address,
                new_customer_with_hash.Phone,
                current_records_with_hash.OrderId,
                current_records_with_hash.OrderDate,
                current_records_with_hash.Amount,
                current_time.alias("EffectiveDate"),
                lit(None).cast("timestamp").alias("EndDate"),
                lit(True).alias("IsCurrent")
            )
            
            # Identify records that haven't changed
            unchanged_records = current_records.join(
                changed_records,
                current_records.CustId == changed_records.CustId,
                "leftanti"
            )
            
            # Combine all records for the updated table
            updated_data = unchanged_records \
                .union(expired_records.drop("customer_hash")) \
                .union(new_current_records)
                
            # Write back to the ordersummary table
            updated_data.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
                
            logger.info(f"Successfully updated ordersummary table with {new_current_records.count()} changed records")
            
        except Exception as e:
            logger.error(f"Error in SCD Type 2 update: {str(e)}")
            raise
            
    def run_etl_pipeline(self) -> None:
        """
        Run the complete ETL pipeline
        """
        try:
            # Step 1: Ingest and cleanse data
            cleansed_customer_df, cleansed_order_df = self.ingest_and_cleanse_data()
            
            # Step 2: Create ordersummary table if it doesn't exist
            self.create_ordersummary_table()
            
            # Check if ordersummary table is empty
            ordersummary_count = self.spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary").count()
            
            if ordersummary_count == 0:
                # Step 3: Initial load of ordersummary
                self.load_initial_ordersummary(cleansed_customer_df, cleansed_order_df)
            else:
                # Step 4: Update ordersummary with SCD Type 2 logic
                self.update_ordersummary_scd2(cleansed_customer_df)
                
            logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            # Send notification on job failure
            self._send_failure_notification(str(e))
            raise
            
    def _send_failure_notification(self, error_message: str) -> None:
        """
        Send notification on job failure (simulated)
        
        Args:
            error_message: Error message to include in notification
        """
        logger.info(f"NOTIFICATION: ETL job failed with error: {error_message}")
        # In a real implementation, this would send an email, Slack message, etc.