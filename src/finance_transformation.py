"""
Finance Data Transformation Module

This module contains the main logic for transforming finance data from FAGLFLEXA and BSEG tables
into the target Finance table according to TR-FIN-001 requirements.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType
import logging
from datetime import datetime
from src.utils import setup_logger, notify_stakeholders

# Set up logging
logger = setup_logger("finance_transformation")

def create_spark_session():
    """
    Create and configure the Spark session.
    
    Returns:
        SparkSession: Configured Spark session
    """
    try:
        spark = SparkSession.builder \
            .appName("Finance Data Transformation") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        error_msg = f"Failed to create Spark session: {str(e)}"
        logger.error(error_msg)
        notify_stakeholders("Finance Data Transformation", error_msg)
        raise

def load_source_data(spark, fiscal_year, posting_period):
    """
    Load source data from FAGLFLEXA, BSEG, and Golden Views.
    
    Args:
        spark (SparkSession): Spark session
        fiscal_year (str): Fiscal year parameter
        posting_period (str): Posting period parameter
        
    Returns:
        tuple: Tuple containing DataFrames for FAGLFLEXA, BSEG, and Golden Views
    """
    try:
        logger.info(f"Loading source data for fiscal year {fiscal_year} and posting period {posting_period}")
        
        # Load FAGLFLEXA data
        faglflexa_df = spark.table("Everest_ECC.FAGLFLEXA")
        logger.info(f"FAGLFLEXA record count: {faglflexa_df.count()}")
        
        # Load BSEG data
        bseg_df = spark.table("Everest_ECC.BSEG")
        logger.info(f"BSEG record count: {bseg_df.count()}")
        
        # Load Golden Views
        entity_golden_view = spark.table("Golden_Views.Entity")
        gl_golden_view = spark.table("Golden_Views.GL")
        trading_partner_golden_view = spark.table("Golden_Views.TradingPartner")
        
        # Load BPC exchange rates
        bpc_exchange_rates = spark.table("Golden_Views.BPC_ExchangeRates")
        
        return faglflexa_df, bseg_df, entity_golden_view, gl_golden_view, trading_partner_golden_view, bpc_exchange_rates
    
    except Exception as e:
        error_msg = f"Error loading source data: {str(e)}"
        logger.error(error_msg)
        notify_stakeholders("Finance Data Transformation", error_msg)
        raise

def transform_finance_data(spark, fiscal_year, posting_period):
    """
    Main transformation function to process finance data.
    
    Args:
        spark (SparkSession): Spark session
        fiscal_year (str): Fiscal year parameter
        posting_period (str): Posting period parameter
        
    Returns:
        DataFrame: Transformed finance data
    """
    try:
        logger.info(f"Starting finance data transformation for FY {fiscal_year}, Period {posting_period}")
        
        # Load source data
        faglflexa_df, bseg_df, entity_golden_view, gl_golden_view, trading_partner_golden_view, bpc_exchange_rates = \
            load_source_data(spark, fiscal_year, posting_period)
        
        # Stage 1: Filter FAGLFLEXA records based on RLDNR = '0L'
        filtered_faglflexa = faglflexa_df.filter(F.col("RLDNR") == "0L")
        logger.info(f"Filtered FAGLFLEXA record count: {filtered_faglflexa.count()}")
        
        # Stage 2: Join FAGLFLEXA with BSEG
        join_condition = (
            (filtered_faglflexa.DOCNR == bseg_df.BELNR) & 
            (filtered_faglflexa.RBUKRS == bseg_df.BUKRS) & 
            (filtered_faglflexa.RYEAR == bseg_df.GJAHR) & 
            (filtered_faglflexa.XBILK == "X")
        )
        
        joined_df = filtered_faglflexa.join(
            bseg_df,
            join_condition,
            "left"
        )
        
        logger.info(f"Joined data record count: {joined_df.count()}")
        
        # Stage 3: Apply transformations
        
        # Filter out company codes starting with '8'
        filtered_df = joined_df.filter(~F.col("RBUKRS").like("8%"))
        
        # Join with Golden Views for mapping
        df_with_entity = filtered_df.join(
            entity_golden_view,
            filtered_df.RBUKRS == entity_golden_view.CompanyCode,
            "left"
        )
        
        df_with_gl = df_with_entity.join(
            gl_golden_view,
            df_with_entity.RACCT == gl_golden_view.SourceGLAccount,
            "left"
        )
        
        df_with_tp = df_with_gl.join(
            trading_partner_golden_view,
            df_with_gl.RASSC == trading_partner_golden_view.SourceTradingPartner,
            "left"
        )
        
        # Filter for realized and unrealized accounts
        realized_unrealized_accounts = gl_golden_view.filter(
            (F.col("AccountType") == "Realized") | (F.col("AccountType") == "Unrealized")
        ).select("SourceGLAccount").distinct()
        
        df_with_realized_unrealized = df_with_tp.join(
            realized_unrealized_accounts,
            df_with_tp.RACCT == realized_unrealized_accounts.SourceGLAccount,
            "inner"
        )
        
        # Select distinct document numbers with realized/unrealized accounts
        distinct_docs = df_with_realized_unrealized.select("DOCNR").distinct()
        
        # Filter original dataset to include only those document numbers
        final_df = df_with_tp.join(
            distinct_docs,
            df_with_tp.DOCNR == distinct_docs.DOCNR,
            "inner"
        )
        
        # Apply final transformations
        transformed_df = final_df.withColumn("FiscalYear", F.lit(fiscal_year)) \
            .withColumn("PostingPeriod", F.lit(posting_period)) \
            .withColumn("SourceFiscalYear", F.col("RYEAR")) \
            .withColumn("SourcePeriod", F.col("POPER")) \
            .withColumn("DocumentNumber", F.col("DOCNR")) \
            .withColumn("CompCode", F.col("RBUKRS")) \
            .withColumn("LegalEntity", F.col("GoldenEntity")) \
            .withColumn("GLAccount", F.col("RACCT")) \
            .withColumn("GoldenGLAcct", F.col("GoldenGLAccount")) \
            .withColumn("TradingPartner", F.col("RASSC")) \
            .withColumn("GoldenTradingPartner", F.col("GoldenTradingPartner")) \
            .withColumn("LocalCurrency", F.col("EntityCurrency")) \
            .withColumn("GainLossLC", F.col("HSL")) \
            .withColumn("TransactionCurrency", F.col("RWCUR")) \
            .withColumn("GainLossTC", F.col("TSL"))
        
        # Calculate GainLossGC using BPC exchange rates
        transformed_df = transformed_df.join(
            bpc_exchange_rates,
            (transformed_df.LocalCurrency == bpc_exchange_rates.FromCurrency) &
            (bpc_exchange_rates.ToCurrency == "USD") &
            (transformed_df.FiscalYear == bpc_exchange_rates.FiscalYear) &
            (transformed_df.PostingPeriod == bpc_exchange_rates.Period),
            "left"
        ).withColumn(
            "GainLossGC", 
            F.col("GainLossLC") * F.col("ExchangeRate")
        )
        
        # Determine offset account logic
        transformed_df = transformed_df.withColumn(
            "OffsetAccount",
            F.when(
                (F.col("AccountType") == "Realized") | (F.col("AccountType") == "Unrealized"),
                F.col("HKONT")
            ).otherwise(None)
        )
        
        # Get Golden Offset Account
        transformed_df = transformed_df.join(
            gl_golden_view.alias("offset_gl"),
            transformed_df.OffsetAccount == F.col("offset_gl.SourceGLAccount"),
            "left"
        ).withColumn(
            "GoldenOffsetAccount",
            F.col("offset_gl.GoldenGLAccount")
        )
        
        # Add remaining fields
        final_transformed_df = transformed_df.withColumn(
            "OffsetAccountLCAmount", F.col("DMBTR")
        ).withColumn(
            "OffsetAccountTCAmount", F.col("WRBTR")
        ).withColumn(
            "OffsetClearingDocumentNumber", F.col("AUGBL")
        ).withColumn(
            "SourceSystem", F.lit("ECC Everest")
        )
        
        # Select only the required columns for the final output
        output_columns = [
            "FiscalYear", "PostingPeriod", "SourceFiscalYear", "SourcePeriod",
            "DocumentNumber", "CompCode", "LegalEntity", "GLAccount", 
            "GoldenGLAcct", "TradingPartner", "GoldenTradingPartner",
            "GainLossGC", "GainLossLC", "LocalCurrency", "GainLossTC",
            "TransactionCurrency", "OffsetAccount", "GoldenOffsetAccount",
            "OffsetAccountLCAmount", "OffsetAccountTCAmount",
            "OffsetClearingDocumentNumber", "SourceSystem"
        ]
        
        result_df = final_transformed_df.select(output_columns)
        
        # Data validation
        validate_data(result_df)
        
        logger.info(f"Transformation completed successfully. Result record count: {result_df.count()}")
        
        return result_df
        
    except Exception as e:
        error_msg = f"Error during finance data transformation: {str(e)}"
        logger.error(error_msg)
        notify_stakeholders("Finance Data Transformation", error_msg)
        raise

def validate_data(df):
    """
    Perform data validation checks on the transformed data.
    
    Args:
        df (DataFrame): Transformed data to validate
    """
    try:
        logger.info("Performing data validation checks")
        
        # Check for null values in critical columns
        null_counts = {}
        critical_columns = ["DocumentNumber", "CompCode", "LegalEntity", "GLAccount", "GoldenGLAcct"]
        
        for col in critical_columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
            
            if null_count > 0:
                logger.warning(f"Column {col} has {null_count} null values")
        
        # Check for data consistency
        currency_mismatch = df.filter(
            (F.col("LocalCurrency").isNotNull()) & 
            (F.col("GainLossLC").isNotNull()) & 
            (F.col("GainLossLC") != 0) & 
            (F.col("GainLossGC").isNull())
        ).count()
        
        if currency_mismatch > 0:
            logger.warning(f"Found {currency_mismatch} rows with LC values but missing GC values")
        
        # Return validation results
        return {
            "null_counts": null_counts,
            "currency_mismatch": currency_mismatch
        }
    
    except Exception as e:
        error_msg = f"Error during data validation: {str(e)}"
        logger.error(error_msg)
        raise

def save_to_target(spark, transformed_df):
    """
    Save the transformed data to the target Finance table.
    
    Args:
        spark (SparkSession): Spark session
        transformed_df (DataFrame): Transformed finance data
    """
    try:
        logger.info("Saving transformed data to target Finance table")
        
        # Save to target table
        transformed_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("Target.Finance")
        
        logger.info("Data successfully saved to Target.Finance table")
        
        # Log row count for verification
        row_count = spark.table("Target.Finance").count()
        logger.info(f"Target.Finance table row count: {row_count}")
        
    except Exception as e:
        error_msg = f"Error saving data to target: {str(e)}"
        logger.error(error_msg)
        notify_stakeholders("Finance Data Transformation", error_msg)
        raise

def main(fiscal_year, posting_period):
    """
    Main execution function for the finance data transformation.
    
    Args:
        fiscal_year (str): Fiscal year parameter
        posting_period (str): Posting period parameter
    """
    start_time = datetime.now()
    logger.info(f"Starting finance data transformation job at {start_time}")
    logger.info(f"Parameters - Fiscal Year: {fiscal_year}, Posting Period: {posting_period}")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Transform data
        transformed_df = transform_finance_data(spark, fiscal_year, posting_period)
        
        # Save to target
        save_to_target(spark, transformed_df)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Finance data transformation job completed successfully at {end_time}")
        logger.info(f"Total duration: {duration} seconds")
        
    except Exception as e:
        error_msg = f"Finance data transformation job failed: {str(e)}"
        logger.error(error_msg)
        notify_stakeholders("Finance Data Transformation", error_msg)
        raise

if __name__ == "__main__":
    # These parameters would typically be passed as job parameters in Databricks
    fiscal_year = "2023"
    posting_period = "12"
    
    main(fiscal_year, posting_period)