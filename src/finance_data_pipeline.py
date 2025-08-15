"""
Finance Data Pipeline

This module implements a data pipeline to load finance data from ECC Everest into the target table "Finance"
based on the specified transformation logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce, concat, expr, current_timestamp
from pyspark.sql.types import StringType
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
EMAIL_CONFIG = {
    'smtp_server': 'smtp.company.com',
    'port': 587,
    'sender': 'finance-pipeline@company.com',
    'recipients': ['admin@company.com', 'finance-team@company.com'],
    'subject': 'Finance Data Pipeline Alert'
}

class FinanceDataPipeline:
    """Finance data pipeline implementation."""
    
    def __init__(self, spark=None):
        """Initialize the pipeline with a SparkSession."""
        self.spark = spark or SparkSession.builder.appName("Finance Data Pipeline").getOrCreate()
        logger.info("Finance Data Pipeline initialized")
    
    def send_email_notification(self, subject, message):
        """Send email notification for critical errors."""
        try:
            msg = MIMEMultipart()
            msg['From'] = EMAIL_CONFIG['sender']
            msg['To'] = ', '.join(EMAIL_CONFIG['recipients'])
            msg['Subject'] = f"{EMAIL_CONFIG['subject']}: {subject}"
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['port'])
            server.starttls()
            # In production, use secrets management for credentials
            # server.login(username, password)
            server.send_message(msg)
            server.quit()
            logger.info("Email notification sent successfully")
        except Exception as e:
            logger.error(f"Failed to send email notification: {str(e)}")
    
    def extract_data(self):
        """Extract data from source tables with retry mechanism."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logger.info("Extracting data from source tables")
                
                # Extract data from FAGLFLEXA table with filter RLDNR = 0L
                faglflexa_df = self.spark.read.format("jdbc") \
                    .option("url", "jdbc:sap://ecc-everest:port/ECC") \
                    .option("dbtable", "FAGLFLEXA") \
                    .option("user", "username") \
                    .option("password", "password") \
                    .load() \
                    .filter(col("RLDNR") == "0L")
                
                # Extract data from BSEG table
                bseg_df = self.spark.read.format("jdbc") \
                    .option("url", "jdbc:sap://ecc-everest:port/ECC") \
                    .option("dbtable", "BSEG") \
                    .option("user", "username") \
                    .option("password", "password") \
                    .load()
                
                # Extract data from golden views
                entity_gv = self.spark.read.table("golden_views.entity")
                gl_gv = self.spark.read.table("golden_views.gl_account")
                trading_partner_gv = self.spark.read.table("golden_views.trading_partner")
                
                logger.info("Data extraction completed successfully")
                return {
                    'faglflexa': faglflexa_df,
                    'bseg': bseg_df,
                    'entity': entity_gv,
                    'gl_account': gl_gv,
                    'trading_partner': trading_partner_gv
                }
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Data extraction failed (attempt {retry_count}/{max_retries}): {str(e)}")
                if retry_count >= max_retries:
                    error_msg = f"Data extraction failed after {max_retries} attempts: {str(e)}"
                    logger.critical(error_msg)
                    self.send_email_notification("Data Extraction Failed", error_msg)
                    raise RuntimeError(error_msg)
    
    def transform_data(self, data_sources):
        """Transform data according to business requirements."""
        try:
            logger.info("Starting data transformation")
            
            # Step 1: Data is already filtered with RLDNR = 0L during extraction
            faglflexa_df = data_sources['faglflexa']
            bseg_df = data_sources['bseg']
            entity_gv = data_sources['entity']
            gl_gv = data_sources['gl_account']
            trading_partner_gv = data_sources['trading_partner']
            
            # Step 2: Join FAGLFLEXA and BSEG tables
            joined_df = faglflexa_df.join(
                bseg_df,
                (faglflexa_df.DOCNR == bseg_df.BELNR) & 
                (faglflexa_df.RBUKRS == bseg_df.BUKRS) & 
                (faglflexa_df.RYEAR == bseg_df.GJAHR),
                "inner"
            ).filter(bseg_df.XBILK == "X")
            
            # Step 3: Apply transformations to create target fields
            transformed_df = joined_df.select(
                # Basic transformations according to FRD
                col("RYEAR").alias("FiscalYear"),
                col("POPER").alias("PostingPeriod"),
                col("RYEAR").alias("SourceFiscalYear"),
                col("POPER").alias("SourcePostingPeriod"),
                col("RBUKRS").alias("CompanyCode"),
                col("RACCT").alias("GLAccount"),
                col("RCNTR").alias("CostCenter"),
                col("PRCTR").alias("ProfitCenter"),
                # Using when clauses for conditional logic
                when(col("BSEG.SHKZG") == "H", -1 * col("BSEG.DMBTR"))
                .otherwise(col("BSEG.DMBTR")).alias("Amount"),
                when(col("BSEG.SHKZG") == "H", -1 * col("BSEG.WRBTR"))
                .otherwise(col("BSEG.WRBTR")).alias("AmountInDocumentCurrency"),
                col("BSEG.WAERS").alias("DocumentCurrency"),
                col("DOCNR").alias("DocumentNumber"),
                col("BSEG.BUZEI").alias("LineItem"),
                col("BSEG.BUPLA").alias("BusinessPlace"),
                col("BSEG.ZUONR").alias("Assignment"),
                col("BSEG.SGTXT").alias("Text"),
                # Concatenate fields for unique identifier
                concat(col("DOCNR"), lit("_"), col("BSEG.BUZEI")).alias("UniqueId"),
                # Add metadata
                current_timestamp().alias("LoadTimestamp"),
                lit("ECC Everest").alias("DataSource")
            )
            
            # Join with golden views to enrich data
            enriched_df = transformed_df.join(
                entity_gv,
                transformed_df.CompanyCode == entity_gv.CompanyCode,
                "left"
            ).join(
                gl_gv,
                transformed_df.GLAccount == gl_gv.GLAccountNumber,
                "left"
            ).join(
                trading_partner_gv,
                transformed_df.CompanyCode == trading_partner_gv.PartnerCompanyCode,
                "left"
            ).select(
                transformed_df["*"],
                entity_gv.EntityName,
                entity_gv.EntityRegion,
                gl_gv.GLAccountName,
                gl_gv.GLAccountType,
                trading_partner_gv.PartnerName
            )
            
            logger.info("Data transformation completed successfully")
            return enriched_df
            
        except Exception as e:
            error_msg = f"Data transformation failed: {str(e)}"
            logger.error(error_msg)
            self.send_email_notification("Data Transformation Failed", error_msg)
            raise
    
    def load_data(self, transformed_df):
        """Load transformed data into target table."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logger.info("Loading data into target table")
                
                # Write to target table
                transformed_df.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable("target_database.Finance")
                
                # Get count for logging
                row_count = transformed_df.count()
                logger.info(f"Data loaded successfully. {row_count} rows written to target table.")
                return row_count
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Data loading failed (attempt {retry_count}/{max_retries}): {str(e)}")
                if retry_count >= max_retries:
                    error_msg = f"Data loading failed after {max_retries} attempts: {str(e)}"
                    logger.critical(error_msg)
                    self.send_email_notification("Data Loading Failed", error_msg)
                    raise RuntimeError(error_msg)
    
    def run_pipeline(self):
        """Execute the complete data pipeline."""
        start_time = datetime.now()
        logger.info(f"Starting Finance Data Pipeline at {start_time}")
        
        try:
            # Extract data from source systems
            data_sources = self.extract_data()
            
            # Transform data according to business requirements
            transformed_df = self.transform_data(data_sources)
            
            # Load data into target table
            rows_loaded = self.load_data(transformed_df)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Finance Data Pipeline completed successfully at {end_time}. Duration: {duration} seconds. Rows loaded: {rows_loaded}")
            
            return {
                "status": "success",
                "rows_processed": rows_loaded,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration
            }
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            error_msg = f"Finance Data Pipeline failed: {str(e)}. Duration: {duration} seconds."
            logger.critical(error_msg)
            self.send_email_notification("Pipeline Execution Failed", error_msg)
            
            return {
                "status": "failed",
                "error": str(e),
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration
            }

def main():
    """Main entry point for the pipeline."""
    # Create SparkSession with appropriate configuration
    spark = SparkSession.builder \
        .appName("Finance Data Pipeline") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "5") \
        .getOrCreate()
    
    # Initialize and run the pipeline
    pipeline = FinanceDataPipeline(spark)
    result = pipeline.run_pipeline()
    
    # Log the result
    if result["status"] == "success":
        logger.info(f"Pipeline executed successfully. Processed {result['rows_processed']} rows.")
    else:
        logger.error(f"Pipeline execution failed: {result['error']}")
    
    return result

if __name__ == "__main__":
    main()