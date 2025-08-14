# Databricks notebook source
# MAGIC %md
# MAGIC # Finance Data Processing Job
# MAGIC 
# MAGIC **Technical Requirement ID**: TR-FIN-001  
# MAGIC **Related Functional Requirement(s)**: FR-FIN-001  
# MAGIC **Objective**: Transform and load data from Everest ECC source tables into the Finance table
# MAGIC 
# MAGIC ## Job Flow:
# MAGIC 1. Retrieve source data from FAGLFLEXA and BSEG tables
# MAGIC 2. Apply filters and derive fields
# MAGIC 3. Calculate GainLossGC and GainLossLC
# MAGIC 4. Derive additional fields
# MAGIC 5. Load transformed data into the Finance table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
import logging

# Set up logging
log_path = "/dbfs/mnt/logs/finance_data_processing/"
log_file = f"{log_path}finance_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Create logger
logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger("Finance_Data_Processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

try:
    # Read FAGLFLEXA table from Everest ECC
    faglflexa_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/mnt/everest_ecc/FAGLFLEXA/")
    
    logger.info("Successfully loaded FAGLFLEXA table")
    
    # Read BSEG table from Everest ECC
    bseg_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/mnt/everest_ecc/BSEG/")
    
    logger.info("Successfully loaded BSEG table")
    
    # Read Golden Entity view
    entity_df = spark.read.format("parquet") \
        .load("/mnt/golden/entity/")
    
    logger.info("Successfully loaded Golden Entity view")
    
    # Read GL view
    gl_df = spark.read.format("parquet") \
        .load("/mnt/golden/gl/")
    
    logger.info("Successfully loaded GL view")
    
    # Read Trading Partner view
    trading_partner_df = spark.read.format("parquet") \
        .load("/mnt/golden/trading_partner/")
    
    logger.info("Successfully loaded Trading Partner view")
    
    # Read Exchange Rate view
    exchange_rate_df = spark.read.format("parquet") \
        .load("/mnt/bpc/s_shared/v_actual_exchange_rate_bpc/")
    
    logger.info("Successfully loaded Exchange Rate view")
    
except Exception as e:
    error_msg = f"Error loading source data: {str(e)}"
    logger.error(error_msg)
    raise Exception(error_msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Join FAGLFLEXA with BSEG on document number, fiscal year, and company code for records with XBILK = 'X'

# COMMAND ----------

try:
    # Register temporary views for SQL operations
    faglflexa_df.createOrReplaceTempView("FAGLFLEXA")
    bseg_df.createOrReplaceTempView("BSEG")
    entity_df.createOrReplaceTempView("ENTITY")
    gl_df.createOrReplaceTempView("GL")
    trading_partner_df.createOrReplaceTempView("TRADING_PARTNER")
    exchange_rate_df.createOrReplaceTempView("EXCHANGE_RATE")
    
    logger.info("Created temporary views for all source tables")
except Exception as e:
    error_msg = f"Error creating temporary views: {str(e)}"
    logger.error(error_msg)
    raise Exception(error_msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Join FAGLFLEXA with BSEG on document number, fiscal year, and company code for records with XBILK = 'X'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW joined_data AS
# MAGIC SELECT 
# MAGIC     F.RLDNR AS Ledger,
# MAGIC     F.RBUKRS AS CompCode,
# MAGIC     F.GJAHR AS FiscalYear,
# MAGIC     F.POPER AS Period,
# MAGIC     CONCAT(F.GJAHR, LPAD(F.POPER, 2, '0')) AS FiscalYearPeriod,
# MAGIC     F.DOCNR AS DocumentNumber,
# MAGIC     F.RRCTY AS RecordType,
# MAGIC     F.RACCT AS GLAccount,
# MAGIC     F.RCNTR AS CostCenter,
# MAGIC     F.PRCTR AS ProfitCenter,
# MAGIC     F.RFAREA AS FunctionalArea,
# MAGIC     F.RBUSA AS BusinessArea,
# MAGIC     F.KOKRS AS ControllingArea,
# MAGIC     F.PRCTR AS Segment,
# MAGIC     F.HSLVT AS AmountLC,
# MAGIC     F.HSL AS AmountGC,
# MAGIC     F.RHCUR AS LocalCurrency,
# MAGIC     F.RKCUR AS GroupCurrency,
# MAGIC     F.RUNIT AS BaseUnit,
# MAGIC     F.RTCUR AS TransactionCurrency,
# MAGIC     F.TSL AS AmountTC,
# MAGIC     B.XBILK,
# MAGIC     B.BSCHL AS PostingKey,
# MAGIC     B.ZUONR AS Assignment,
# MAGIC     B.SGTXT AS ItemText
# MAGIC FROM 
# MAGIC     FAGLFLEXA F
# MAGIC JOIN 
# MAGIC     BSEG B
# MAGIC ON 
# MAGIC     F.DOCNR = B.BELNR 
# MAGIC     AND F.GJAHR = B.GJAHR 
# MAGIC     AND F.RBUKRS = B.BUKRS
# MAGIC WHERE 
# MAGIC     B.XBILK = 'X'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Filter out records with CompCode starting with '8%'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Filter out records with CompCode starting with '8%'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW filtered_data AS
# MAGIC SELECT *
# MAGIC FROM joined_data
# MAGIC WHERE CompCode NOT LIKE '8%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Derive Legal Entity by reading Entity golden view and exclude archived Golden Entity values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Derive Legal Entity from Golden Entity view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW data_with_entity AS
# MAGIC SELECT 
# MAGIC     F.*,
# MAGIC     E.EntityID,
# MAGIC     E.EntityName,
# MAGIC     E.LegalEntityName
# MAGIC FROM 
# MAGIC     filtered_data F
# MAGIC LEFT JOIN 
# MAGIC     ENTITY E
# MAGIC ON 
# MAGIC     F.CompCode = E.CompanyCode
# MAGIC WHERE 
# MAGIC     E.IsArchived = FALSE OR E.IsArchived IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Derive GLAccount by selecting distinct FAGLFLEXA.DOCNR where FAGLFLEXA.RACCT belongs to golden acct realized and unrealized

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Derive GLAccount information from GL golden view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW data_with_gl AS
# MAGIC SELECT 
# MAGIC     D.*,
# MAGIC     G.GoldenGLAccount,
# MAGIC     G.GLAccountName,
# MAGIC     G.GLAccountType,
# MAGIC     G.IsRealizedAccount,
# MAGIC     G.IsUnrealizedAccount
# MAGIC FROM 
# MAGIC     data_with_entity D
# MAGIC LEFT JOIN 
# MAGIC     GL G
# MAGIC ON 
# MAGIC     D.GLAccount = G.SourceGLAccount
# MAGIC WHERE
# MAGIC     (G.IsRealizedAccount = TRUE OR G.IsUnrealizedAccount = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Calculate GainLossGC using BPC exchange rates and FiscalYearPeriod

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 5: Calculate GainLossGC using BPC exchange rates
# MAGIC CREATE OR REPLACE TEMPORARY VIEW data_with_gainloss_gc AS
# MAGIC SELECT 
# MAGIC     D.*,
# MAGIC     CASE 
# MAGIC         WHEN D.LocalCurrency = D.GroupCurrency THEN 0
# MAGIC         ELSE D.AmountLC * ER.ExchangeRate - D.AmountGC
# MAGIC     END AS GainLossGC
# MAGIC FROM 
# MAGIC     data_with_gl D
# MAGIC LEFT JOIN 
# MAGIC     EXCHANGE_RATE ER
# MAGIC ON 
# MAGIC     D.FiscalYearPeriod = ER.FiscalYearPeriod
# MAGIC     AND D.LocalCurrency = ER.FromCurrency
# MAGIC     AND D.GroupCurrency = ER.ToCurrency

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Derive GainLossLC by applying logic based on LocalCurrency

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 6: Derive GainLossLC based on LocalCurrency
# MAGIC CREATE OR REPLACE TEMPORARY VIEW data_with_gainloss_lc AS
# MAGIC SELECT 
# MAGIC     D.*,
# MAGIC     CASE 
# MAGIC         WHEN D.LocalCurrency = D.TransactionCurrency THEN 0
# MAGIC         ELSE D.AmountTC * ER.ExchangeRate - D.AmountLC
# MAGIC     END AS GainLossLC
# MAGIC FROM 
# MAGIC     data_with_gainloss_gc D
# MAGIC LEFT JOIN 
# MAGIC     EXCHANGE_RATE ER
# MAGIC ON 
# MAGIC     D.FiscalYearPeriod = ER.FiscalYearPeriod
# MAGIC     AND D.TransactionCurrency = ER.FromCurrency
# MAGIC     AND D.LocalCurrency = ER.ToCurrency

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Derive additional fields such as GainLossTC, OffsetAccount, and GoldenOffsetAccount

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 7: Derive additional fields
# MAGIC CREATE OR REPLACE TEMPORARY VIEW final_data AS
# MAGIC SELECT 
# MAGIC     D.*,
# MAGIC     CASE 
# MAGIC         WHEN D.TransactionCurrency = D.GroupCurrency THEN 0
# MAGIC         ELSE D.AmountTC * ER.ExchangeRate - D.AmountGC
# MAGIC     END AS GainLossTC,
# MAGIC     CASE 
# MAGIC         WHEN D.PostingKey IN ('40', '50') THEN 
# MAGIC             (SELECT MIN(RACCT) FROM FAGLFLEXA WHERE DOCNR = D.DocumentNumber AND RACCT != D.GLAccount)
# MAGIC         ELSE NULL
# MAGIC     END AS OffsetAccount,
# MAGIC     CASE 
# MAGIC         WHEN D.PostingKey IN ('40', '50') THEN 
# MAGIC             (SELECT MIN(G.GoldenGLAccount) 
# MAGIC              FROM GL G 
# MAGIC              JOIN FAGLFLEXA F ON G.SourceGLAccount = F.RACCT 
# MAGIC              WHERE F.DOCNR = D.DocumentNumber AND F.RACCT != D.GLAccount)
# MAGIC         ELSE NULL
# MAGIC     END AS GoldenOffsetAccount,
# MAGIC     CURRENT_TIMESTAMP() AS LoadTimestamp
# MAGIC FROM 
# MAGIC     data_with_gainloss_lc D
# MAGIC LEFT JOIN 
# MAGIC     EXCHANGE_RATE ER
# MAGIC ON 
# MAGIC     D.FiscalYearPeriod = ER.FiscalYearPeriod
# MAGIC     AND D.TransactionCurrency = ER.FromCurrency
# MAGIC     AND D.GroupCurrency = ER.ToCurrency

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Final Data to Target Table

# COMMAND ----------

try:
    # Get the final dataframe
    final_df = spark.table("final_data")
    
    # Write to target Finance table in Parquet format
    final_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("Finance.FinanceGainLoss")
    
    logger.info("Successfully wrote data to Finance.FinanceGainLoss table")
    
    # Record count validation
    source_count = spark.table("joined_data").count()
    target_count = spark.table("Finance.FinanceGainLoss").count()
    
    logger.info(f"Source record count: {source_count}")
    logger.info(f"Target record count: {target_count}")
    
    if target_count == 0:
        error_msg = "Target table has 0 records. Job completed but data may be missing."
        logger.error(error_msg)
        raise Exception(error_msg)
        
except Exception as e:
    error_msg = f"Error writing to target table: {str(e)}"
    logger.error(error_msg)
    raise Exception(error_msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display summary statistics
# MAGIC SELECT 
# MAGIC     COUNT(*) AS TotalRecords,
# MAGIC     COUNT(DISTINCT CompCode) AS UniqueCompanyCodes,
# MAGIC     COUNT(DISTINCT EntityID) AS UniqueEntities,
# MAGIC     COUNT(DISTINCT FiscalYearPeriod) AS UniqueFiscalPeriods,
# MAGIC     SUM(CASE WHEN GainLossGC != 0 THEN 1 ELSE 0 END) AS RecordsWithGainLossGC,
# MAGIC     SUM(CASE WHEN GainLossLC != 0 THEN 1 ELSE 0 END) AS RecordsWithGainLossLC,
# MAGIC     SUM(CASE WHEN GainLossTC != 0 THEN 1 ELSE 0 END) AS RecordsWithGainLossTC
# MAGIC FROM Finance.FinanceGainLoss

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling and Logging Summary

# COMMAND ----------

# Log job completion
logger.info(f"Finance Data Processing Job completed successfully at {datetime.now()}")

# Display job metrics
print("Job Execution Summary:")
print(f"- Start Time: {datetime.now()}")
print(f"- Records Processed: {target_count}")
print(f"- Job Status: Completed Successfully")
print(f"- Log File: {log_file}")