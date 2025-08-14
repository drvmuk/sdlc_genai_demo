"""
Finance Data Transformation Module

This module implements the finance data transformation logic as per TR-FIN-001.
It processes data from FAGLFLEXA and BSEG tables, applies transformations,
and stores the results in the target Finance table.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, expr, coalesce
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("finance_transformation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "source_schema": "everest_ecc",
    "golden_views_schema": "golden_views",
    "target_schema": "finance",
    "target_table": "finance_data",
    "admin_email": "data.admin@company.com",
    "max_retries": 3
}

def get_spark_session():
    """
    Initialize and return a Spark session.
    """
    return (SparkSession.builder
            .appName("Finance Data Transformation")
            .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.default.parallelism", "200")
            .getOrCreate())

def send_notification(subject, message):
    """
    Send email notification for job status.
    
    Args:
        subject (str): Email subject
        message (str): Email message
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = 'finance.data.pipeline@company.com'
        msg['To'] = CONFIG["admin_email"]
        msg['Subject'] = subject
        
        msg.attach(MIMEText(message, 'plain'))
        
        # This would be replaced with actual SMTP server details in production
        logger.info(f"Would send email: {subject} - {message}")
        
        # Simulated email sending
        # with smtplib.SMTP('smtp.company.com', 587) as server:
        #     server.starttls()
        #     server.login('username', 'password')
        #     server.send_message(msg)
    except Exception as e:
        logger.error(f"Failed to send notification: {e}")

def execute_with_retry(func, max_retries=None):
    """
    Execute a function with retry logic.
    
    Args:
        func: Function to execute
        max_retries (int): Maximum number of retries
    
    Returns:
        Result of the function
    """
    if max_retries is None:
        max_retries = CONFIG["max_retries"]
    
    retries = 0
    while retries <= max_retries:
        try:
            return func()
        except Exception as e:
            retries += 1
            if retries > max_retries:
                logger.error(f"Failed after {max_retries} retries: {e}")
                raise
            logger.warning(f"Retry {retries}/{max_retries} after error: {e}")

def transform_finance_data(spark):
    """
    Main function to transform finance data.
    
    Args:
        spark: SparkSession object
    
    Returns:
        DataFrame: Transformed finance data
    """
    logger.info("Starting finance data transformation")
    
    try:
        # Step 1: Retrieve data from FAGLFLEXA and BSEG tables
        logger.info("Retrieving data from FAGLFLEXA and BSEG tables")
        
        faglflexa_df = spark.table(f"{CONFIG['source_schema']}.FAGLFLEXA")
        bseg_df = spark.table(f"{CONFIG['source_schema']}.BSEG")
        
        # Step 2: Join FAGLFLEXA with BSEG and apply filters
        logger.info("Joining FAGLFLEXA with BSEG and applying filters")
        
        joined_df = faglflexa_df.alias("f").join(
            bseg_df.alias("b"),
            (col("f.DOCNR") == col("b.BELNR")) & 
            (col("f.RBUKRS") == col("b.BUKRS")) & 
            (col("f.RYEAR") == col("b.GJAHR")),
            "inner"
        ).filter(
            (col("f.RLDNR") == "0L") & 
            (col("f.XBILK") == "X")
        )
        
        # Step 3: Get golden views for reference data
        logger.info("Retrieving golden views for reference data")
        
        entity_view = spark.table(f"{CONFIG['golden_views_schema']}.v_entity")
        gl_view = spark.table(f"{CONFIG['golden_views_schema']}.v_gl_account")
        trading_partner_view = spark.table(f"{CONFIG['golden_views_schema']}.v_trading_partner")
        realized_unrealized_view = spark.table(f"{CONFIG['golden_views_schema']}.v_realized_unrealized_glaccts")
        bpc_exchange_rates = spark.table(f"{CONFIG['golden_views_schema']}.v_bpc_exchange_rates")
        
        # Step 4: Apply transformation logic
        logger.info("Applying transformation logic")
        
        transformed_df = joined_df.select(
            col("f.RYEAR").alias("FiscalYear"),
            col("f.POPER").alias("PostingPeriod"),
            col("f.DOCNR").alias("DocumentNumber"),
            col("f.RBUKRS").alias("CompCode"),
            # Join with entity view to get LegalEntity
            col("entity_view.LegalEntity").alias("LegalEntity"),
            col("f.RACCT").alias("GLAccount"),
            # Join with gl view to get GoldenGLAcct
            col("gl_view.GoldenGLAcct").alias("GoldenGLAcct"),
            col("f.RCNTR").alias("TradingPartner"),
            # Join with trading partner view to get GoldenTradingPartner
            col("trading_partner_view.GoldenTradingPartner").alias("GoldenTradingPartner"),
            # Will calculate GainLossGC later
            lit(None).alias("GainLossGC"),
            col("f.HSL").alias("GainLossLC"),
            col("f.KSL").alias("GainLossTC"),
            col("f.RHCUR").alias("LocalCurrency"),
            col("f.RTCUR").alias("TransactionCurrency"),
            # Will determine OffsetAccount later
            lit(None).alias("OffsetAccount"),
            lit(None).alias("GoldenOffsetAccount"),
            col("b.DMBTR").alias("OffsetAccountLCAmount"),
            col("b.AUGBL").alias("OffsetClearingDocumentNumber"),
            lit("Everest ECC").alias("SourceSystem")
        ).join(
            entity_view,
            col("CompCode") == entity_view.CompanyCode,
            "left"
        ).join(
            gl_view,
            col("GLAccount") == gl_view.GLAccount,
            "left"
        ).join(
            trading_partner_view,
            col("TradingPartner") == trading_partner_view.TradingPartner,
            "left"
        )
        
        # Step 5: Calculate GainLossGC
        logger.info("Calculating GainLossGC")
        
        transformed_df = transformed_df.join(
            bpc_exchange_rates,
            (transformed_df.FiscalYear == bpc_exchange_rates.Year) & 
            (transformed_df.PostingPeriod == bpc_exchange_rates.Period) & 
            (transformed_df.LocalCurrency == bpc_exchange_rates.FromCurrency) &
            (lit("USD") == bpc_exchange_rates.ToCurrency),
            "left"
        ).withColumn(
            "GainLossGC",
            col("GainLossLC") * col("bpc_exchange_rates.ExchangeRate")
        )
        
        # Step 6: Determine OffsetAccount
        logger.info("Determining OffsetAccount")
        
        transformed_df = transformed_df.join(
            realized_unrealized_view,
            transformed_df.GLAccount == realized_unrealized_view.GLAccount,
            "left"
        ).withColumn(
            "OffsetAccount",
            when(
                col("realized_unrealized_view.Realized_Unrealized") == "Realized",
                expr("CASE WHEN GainLossLC > 0 THEN '999001' ELSE '999002' END")
            ).when(
                col("realized_unrealized_view.Realized_Unrealized") == "Unrealized",
                expr("CASE WHEN GainLossLC > 0 THEN '999003' ELSE '999004' END")
            ).otherwise(None)
        )
        
        # Join with GL view again to get GoldenOffsetAccount
        transformed_df = transformed_df.join(
            gl_view.alias("offset_gl"),
            col("OffsetAccount") == col("offset_gl.GLAccount"),
            "left"
        ).withColumn(
            "GoldenOffsetAccount",
            col("offset_gl.GoldenGLAcct")
        )
        
        # Select final columns and drop any temporary/intermediate columns
        final_df = transformed_df.select(
            "FiscalYear", "PostingPeriod", "DocumentNumber", "CompCode", "LegalEntity",
            "GLAccount", "GoldenGLAcct", "TradingPartner", "GoldenTradingPartner",
            "GainLossGC", "GainLossLC", "GainLossTC", "LocalCurrency", "TransactionCurrency",
            "OffsetAccount", "GoldenOffsetAccount", "OffsetAccountLCAmount",
            "OffsetClearingDocumentNumber", "SourceSystem"
        )
        
        logger.info("Finance data transformation completed successfully")
        return final_df
        
    except Exception as e:
        logger.error(f"Error in finance data transformation: {e}")
        send_notification(
            "Finance Data Transformation Failed",
            f"The finance data transformation job failed with the following error: {e}"
        )
        raise

def save_finance_data(df):
    """
    Save the transformed finance data to the target table.
    
    Args:
        df: DataFrame containing transformed finance data
    """
    try:
        logger.info(f"Saving finance data to {CONFIG['target_schema']}.{CONFIG['target_table']}")
        
        # Write the data to the target table
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{CONFIG['target_schema']}.{CONFIG['target_table']}")
        
        logger.info("Finance data saved successfully")
        
    except Exception as e:
        logger.error(f"Error saving finance data: {e}")
        send_notification(
            "Failed to Save Finance Data",
            f"The finance data transformation job failed to save data with the following error: {e}"
        )
        raise

def main():
    """
    Main entry point for the finance data transformation job.
    """
    spark = get_spark_session()
    
    try:
        logger.info("Starting finance data transformation job")
        
        # Execute the transformation with retry logic
        transformed_df = execute_with_retry(lambda: transform_finance_data(spark))
        
        # Save the transformed data
        save_finance_data(transformed_df)
        
        logger.info("Finance data transformation job completed successfully")
        send_notification(
            "Finance Data Transformation Successful",
            "The finance data transformation job has completed successfully."
        )
        
    except Exception as e:
        logger.error(f"Finance data transformation job failed: {e}")
        send_notification(
            "Finance Data Transformation Failed",
            f"The finance data transformation job failed with the following error: {e}"
        )
        raise
    finally:
        # Don't stop the SparkSession as it's managed by Databricks

if __name__ == "__main__":
    main()