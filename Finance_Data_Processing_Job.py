# Databricks notebook source
# MAGIC %md
# MAGIC # Finance Data Processing Job (TR-FIN-001)
# MAGIC 
# MAGIC **Objective**: Extract financial data from Everest ECC, apply transformations, and load into Finance target table
# MAGIC 
# MAGIC **Source Data**:
# MAGIC - FAGLFLEXA (Everest ECC)
# MAGIC - BSEG (Everest ECC)
# MAGIC - Entity Golden View
# MAGIC - Golden GL View
# MAGIC - Golden Trading Partner View
# MAGIC 
# MAGIC **Target Data**:
# MAGIC - Finance Target Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Parameters

# COMMAND ----------

# MAGIC %run ../Shared/Config/Finance_Config

# COMMAND ----------

# Import required libraries
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# COMMAND ----------

# Widget setup for parameters
dbutils.widgets.text("FiscalYear", "", "Fiscal Year")
dbutils.widgets.text("PostingPeriod", "", "Posting Period")
dbutils.widgets.text("RunDate", datetime.now().strftime("%Y-%m-%d"), "Run Date")

# Get parameter values
fiscal_year = dbutils.widgets.get("FiscalYear")
posting_period = dbutils.widgets.get("PostingPeriod")
run_date = dbutils.widgets.get("RunDate")

# Validate parameters
if not fiscal_year or not posting_period:
    raise ValueError("FiscalYear and PostingPeriod parameters are required")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging Setup

# COMMAND ----------

# Setup logging
log_path = f"/dbfs/logs/finance/data_processing/{fiscal_year}_{posting_period}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("Finance_Data_Processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1: Data Extraction

# COMMAND ----------

# Function to extract data with error handling and retry
def extract_data_with_retry(query, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            logger.info(f"Extracting data, attempt {retries + 1}")
            result = spark.sql(query)
            logger.info(f"Data extraction successful. Row count: {result.count()}")
            return result
        except Exception as e:
            retries += 1
            logger.error(f"Data extraction failed. Attempt {retries} of {max_retries}. Error: {str(e)}")
            if retries >= max_retries:
                logger.error("Max retries reached. Raising exception.")
                raise
            # Wait before retrying
            import time
            time.sleep(30)  # 30 seconds delay before retry

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify source tables exist
# MAGIC SHOW TABLES IN everest_ecc;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract FAGLFLEXA Data

# COMMAND ----------

try:
    logger.info(f"Starting extraction of FAGLFLEXA data for FY {fiscal_year} and Period {posting_period}")
    
    faglflexa_query = f"""
    SELECT *
    FROM everest_ecc.FAGLFLEXA
    WHERE RLDNR = '0L'
    AND RYEAR = '{fiscal_year}'
    """
    
    faglflexa_df = extract_data_with_retry(faglflexa_query)
    
    # Cache the dataframe for better performance
    faglflexa_df.cache()
    
    logger.info(f"FAGLFLEXA data extracted successfully. Row count: {faglflexa_df.count()}")
    
except Exception as e:
    logger.error(f"Failed to extract FAGLFLEXA data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract BSEG Data

# COMMAND ----------

try:
    logger.info(f"Starting extraction of BSEG data for FY {fiscal_year}")
    
    bseg_query = f"""
    SELECT *
    FROM everest_ecc.BSEG
    WHERE GJAHR = '{fiscal_year}'
    AND XBILK = 'X'
    """
    
    bseg_df = extract_data_with_retry(bseg_query)
    
    # Cache the dataframe for better performance
    bseg_df.cache()
    
    logger.info(f"BSEG data extracted successfully. Row count: {bseg_df.count()}")
    
except Exception as e:
    logger.error(f"Failed to extract BSEG data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract Golden Views Data

# COMMAND ----------

try:
    logger.info("Starting extraction of Entity Golden View data")
    
    entity_golden_view_query = """
    SELECT *
    FROM golden_views.entity
    """
    
    entity_df = extract_data_with_retry(entity_golden_view_query)
    
    logger.info(f"Entity Golden View data extracted successfully. Row count: {entity_df.count()}")
    
except Exception as e:
    logger.error(f"Failed to extract Entity Golden View data: {str(e)}")
    raise

# COMMAND ----------

try:
    logger.info("Starting extraction of GL Golden View data")
    
    gl_golden_view_query = """
    SELECT *
    FROM golden_views.gl_account
    """
    
    gl_df = extract_data_with_retry(gl_golden_view_query)
    
    logger.info(f"GL Golden View data extracted successfully. Row count: {gl_df.count()}")
    
except Exception as e:
    logger.error(f"Failed to extract GL Golden View data: {str(e)}")
    raise

# COMMAND ----------

try:
    logger.info("Starting extraction of Trading Partner Golden View data")
    
    tp_golden_view_query = """
    SELECT *
    FROM golden_views.trading_partner
    """
    
    tp_df = extract_data_with_retry(tp_golden_view_query)
    
    logger.info(f"Trading Partner Golden View data extracted successfully. Row count: {tp_df.count()}")
    
except Exception as e:
    logger.error(f"Failed to extract Trading Partner Golden View data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract Exchange Rate Data

# COMMAND ----------

try:
    logger.info("Starting extraction of Exchange Rate data")
    
    exchange_rate_query = f"""
    SELECT *
    FROM s_shared.v_actual_exchange_rate_bpc
    WHERE fiscal_year = '{fiscal_year}'
    """
    
    exchange_rate_df = extract_data_with_retry(exchange_rate_query)
    
    logger.info(f"Exchange Rate data extracted successfully. Row count: {exchange_rate_df.count()}")
    
except Exception as e:
    logger.error(f"Failed to extract Exchange Rate data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2: Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join FAGLFLEXA and BSEG

# COMMAND ----------

try:
    logger.info("Joining FAGLFLEXA and BSEG tables")
    
    joined_df = faglflexa_df.join(
        bseg_df,
        (faglflexa_df.DOCNR == bseg_df.BELNR) &
        (faglflexa_df.RBUKRS == bseg_df.BUKRS) &
        (faglflexa_df.RYEAR == bseg_df.GJAHR),
        "inner"
    )
    
    # Uncache the individual dataframes to free up memory
    faglflexa_df.unpersist()
    bseg_df.unpersist()
    
    # Cache the joined dataframe
    joined_df.cache()
    
    logger.info(f"FAGLFLEXA and BSEG joined successfully. Row count: {joined_df.count()}")
    
except Exception as e:
    logger.error(f"Failed to join FAGLFLEXA and BSEG: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Transformations

# COMMAND ----------

try:
    logger.info("Applying transformations to the joined data")
    
    # Apply transformations
    transformed_df = joined_df.withColumn("FiscalYear", F.lit(fiscal_year)) \
        .withColumn("PostingPeriod", F.lit(posting_period)) \
        .withColumn("SourceFiscalYear", F.col("RYEAR")) \
        .withColumn("CompanyCode", F.col("RBUKRS")) \
        .withColumn("DocumentNumber", F.col("DOCNR")) \
        .withColumn("LineItem", F.col("DOCLN")) \
        .withColumn("GLAccount", F.col("RACCT")) \
        .withColumn("PostingDate", F.to_date(F.col("BUDAT"), "yyyyMMdd")) \
        .withColumn("DocumentDate", F.to_date(F.col("BLDAT"), "yyyyMMdd")) \
        .withColumn("DocumentType", F.col("BLART")) \
        .withColumn("PostingKey", F.col("BSCHL")) \
        .withColumn("TradingPartner", F.col("VBUND")) \
        .withColumn("Text", F.col("SGTXT")) \
        .withColumn("Reference", F.col("XBLNR")) \
        .withColumn("LocalCurrency", F.col("RHCUR")) \
        .withColumn("AmountLC", F.col("HSL"))
    
    logger.info("Basic transformations applied successfully")
    
except Exception as e:
    logger.error(f"Failed to apply basic transformations: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Map Company Codes, GL Accounts, and Trading Partners

# COMMAND ----------

try:
    logger.info("Mapping company codes, GL accounts, and trading partners")
    
    # Map company codes
    transformed_df = transformed_df.join(
        entity_df.select("company_code", "entity_name", "entity_currency"),
        transformed_df.CompanyCode == entity_df.company_code,
        "left"
    ).withColumnRenamed("entity_name", "EntityName") \
     .withColumnRenamed("entity_currency", "EntityCurrency")
    
    # Map GL accounts
    transformed_df = transformed_df.join(
        gl_df.select("gl_account", "gl_account_name", "account_type"),
        transformed_df.GLAccount == gl_df.gl_account,
        "left"
    ).withColumnRenamed("gl_account_name", "GLAccountName") \
     .withColumnRenamed("account_type", "AccountType")
    
    # Map trading partners
    transformed_df = transformed_df.join(
        tp_df.select("trading_partner_code", "trading_partner_name"),
        transformed_df.TradingPartner == tp_df.trading_partner_code,
        "left"
    ).withColumnRenamed("trading_partner_name", "TradingPartnerName")
    
    logger.info("Mapping completed successfully")
    
except Exception as e:
    logger.error(f"Failed to map reference data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Group Currency Amounts

# COMMAND ----------

try:
    logger.info("Calculating Group Currency amounts")
    
    # Join with exchange rates
    transformed_df = transformed_df.join(
        exchange_rate_df.select("from_currency", "to_currency", "fiscal_year", "period", "rate"),
        (transformed_df.EntityCurrency == exchange_rate_df.from_currency) &
        (F.lit("USD") == exchange_rate_df.to_currency) &
        (transformed_df.FiscalYear == exchange_rate_df.fiscal_year) &
        (transformed_df.PostingPeriod == exchange_rate_df.period),
        "left"
    )
    
    # Calculate GC amounts
    transformed_df = transformed_df.withColumn("ExchangeRate", F.col("rate")) \
        .withColumn("AmountGC", F.col("AmountLC") * F.col("rate")) \
        .withColumn("GainLossGC", 
                    F.when(F.col("EntityCurrency") == "USD", F.lit(0))
                     .otherwise(F.col("AmountGC") - F.col("AmountLC")))
    
    logger.info("Group Currency calculations completed successfully")
    
except Exception as e:
    logger.error(f"Failed to calculate Group Currency amounts: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Transformations and Cleanup

# COMMAND ----------

try:
    logger.info("Performing final transformations and cleanup")
    
    # Select final columns and apply any additional transformations
    final_df = transformed_df.select(
        "FiscalYear",
        "PostingPeriod",
        "SourceFiscalYear",
        "CompanyCode",
        "EntityName",
        "EntityCurrency",
        "DocumentNumber",
        "LineItem",
        "GLAccount",
        "GLAccountName",
        "AccountType",
        "PostingDate",
        "DocumentDate",
        "DocumentType",
        "PostingKey",
        "TradingPartner",
        "TradingPartnerName",
        "Text",
        "Reference",
        "LocalCurrency",
        "AmountLC",
        "ExchangeRate",
        "AmountGC",
        "GainLossGC"
    )
    
    # Add audit columns
    final_df = final_df.withColumn("CreatedDate", F.current_timestamp()) \
                      .withColumn("CreatedBy", F.lit("Databricks_Finance_Job")) \
                      .withColumn("ProcessingDate", F.to_date(F.lit(run_date)))
    
    # Uncache the intermediate dataframe
    transformed_df.unpersist()
    
    # Cache the final dataframe
    final_df.cache()
    
    logger.info(f"Final transformations completed. Row count: {final_df.count()}")
    
except Exception as e:
    logger.error(f"Failed to perform final transformations: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3: Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Target Table

# COMMAND ----------

try:
    logger.info("Starting data loading to Finance target table")
    
    # Count rows before writing
    row_count = final_df.count()
    logger.info(f"Preparing to write {row_count} rows to target table")
    
    # Write to target table
    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("finance.financial_data")
    
    logger.info(f"Successfully loaded {row_count} rows to finance.financial_data")
    
    # Uncache the final dataframe
    final_df.unpersist()
    
except Exception as e:
    logger.error(f"Failed to load data to target table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Data Load

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify data was loaded correctly
# MAGIC SELECT 
# MAGIC   FiscalYear,
# MAGIC   PostingPeriod,
# MAGIC   COUNT(*) as RecordCount
# MAGIC FROM finance.financial_data
# MAGIC WHERE FiscalYear = '${FiscalYear}'
# MAGIC AND PostingPeriod = '${PostingPeriod}'
# MAGIC GROUP BY FiscalYear, PostingPeriod;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Completion

# COMMAND ----------

# Log job completion
logger.info(f"Finance data processing job completed successfully for FY {fiscal_year}, Period {posting_period}")

# COMMAND ----------

# Display job summary
display(spark.sql(f"""
SELECT 
  '{fiscal_year}' as FiscalYear,
  '{posting_period}' as PostingPeriod,
  '{run_date}' as RunDate,
  (SELECT COUNT(*) FROM finance.financial_data WHERE FiscalYear = '{fiscal_year}' AND PostingPeriod = '{posting_period}') as RecordsProcessed,
  current_timestamp() as CompletionTime
"""))