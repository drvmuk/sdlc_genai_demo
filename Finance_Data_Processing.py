# Databricks notebook source
# MAGIC %md
# MAGIC # Finance Data Processing Job
# MAGIC 
# MAGIC **Technical Requirement ID:** TR-FIN-001  
# MAGIC **Related Functional Requirement:** FR-FIN-001
# MAGIC 
# MAGIC This notebook implements the data processing job to load finance data from Everest ECC source tables into the Finance table, applying specified filters, joins, and transformation logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set configuration parameters
# MAGIC SET spark.sql.legacy.timeParserPolicy = LEGACY;
# MAGIC SET spark.sql.ansi.enabled = false;

# COMMAND ----------

# MAGIC %python
# MAGIC # Import required libraries
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.window import Window
# MAGIC from datetime import datetime
# MAGIC 
# MAGIC # Define logging function
# MAGIC def log_processing_step(step_name, record_count=None, error=None):
# MAGIC     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# MAGIC     if error:
# MAGIC         print(f"[{timestamp}] ERROR in {step_name}: {error}")
# MAGIC     elif record_count is not None:
# MAGIC         print(f"[{timestamp}] INFO: {step_name} completed with {record_count} records processed")
# MAGIC     else:
# MAGIC         print(f"[{timestamp}] INFO: {step_name} started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Source Data Exploration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore FAGLFLEXA table structure
# MAGIC SELECT * FROM everest_ecc.FAGLFLEXA LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore BSEG table structure
# MAGIC SELECT * FROM everest_ecc.BSEG LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore Entity Golden View
# MAGIC SELECT * FROM golden_views.entity LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore GL Golden View
# MAGIC SELECT * FROM golden_views.gl_account LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore Trading Partner Golden View
# MAGIC SELECT * FROM golden_views.trading_partner LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore BPC Exchange Rates
# MAGIC SELECT * FROM s_shared.v_actual_exchange_rate_bpc LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Processing Pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Retrieve Source Data - Join FAGLFLEXA with BSEG
# MAGIC CREATE OR REPLACE TEMPORARY VIEW finance_source_data AS
# MAGIC SELECT 
# MAGIC   f.*,
# MAGIC   b.BELNR,
# MAGIC   b.BUKRS,
# MAGIC   b.GJAHR,
# MAGIC   b.XBILK,
# MAGIC   b.HKONT,
# MAGIC   b.ZUONR,
# MAGIC   b.SGTXT
# MAGIC FROM everest_ecc.FAGLFLEXA f
# MAGIC INNER JOIN everest_ecc.BSEG b
# MAGIC   ON f.DOCNR = b.BELNR
# MAGIC   AND f.RBUKRS = b.BUKRS
# MAGIC   AND f.RYEAR = b.GJAHR
# MAGIC WHERE b.XBILK = 'X';

# COMMAND ----------

# MAGIC %python
# MAGIC # Log the completion of source data retrieval
# MAGIC try:
# MAGIC     source_data_count = spark.sql("SELECT COUNT(*) as count FROM finance_source_data").collect()[0]["count"]
# MAGIC     log_processing_step("Source Data Retrieval", source_data_count)
# MAGIC except Exception as e:
# MAGIC     log_processing_step("Source Data Retrieval", error=str(e))
# MAGIC     raise e

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Apply Filters based on RLDNR and RBUKRS
# MAGIC CREATE OR REPLACE TEMPORARY VIEW finance_filtered_data AS
# MAGIC SELECT *
# MAGIC FROM finance_source_data
# MAGIC WHERE RLDNR = '0L'
# MAGIC AND RBUKRS NOT LIKE '8%';

# COMMAND ----------

# MAGIC %python
# MAGIC # Log the completion of data filtering
# MAGIC try:
# MAGIC     filtered_data_count = spark.sql("SELECT COUNT(*) as count FROM finance_filtered_data").collect()[0]["count"]
# MAGIC     log_processing_step("Data Filtering", filtered_data_count)
# MAGIC except Exception as e:
# MAGIC     log_processing_step("Data Filtering", error=str(e))
# MAGIC     raise e

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Transform Data - Map and derive columns according to business logic
# MAGIC CREATE OR REPLACE TEMPORARY VIEW finance_transformed_data AS
# MAGIC SELECT
# MAGIC   fd.DOCNR AS SourceDocumentID,
# MAGIC   fd.RYEAR AS SourceFiscalYear,
# MAGIC   fd.POPER AS SourcePeriod,
# MAGIC   fd.RBUKRS AS CompCode,
# MAGIC   e.LegalEntityID AS LegalEntity,
# MAGIC   gl.GoldenGLAccountID AS GoldenGLAcct,
# MAGIC   fd.RACCT AS SourceGLAcct,
# MAGIC   fd.RCNTR AS CostCenter,
# MAGIC   fd.PRCTR AS ProfitCenter,
# MAGIC   fd.RFAREA AS FunctionalArea,
# MAGIC   tp.GoldenTradingPartnerID AS GoldenTradingPartner,
# MAGIC   fd.ZUONR AS Assignment,
# MAGIC   fd.SGTXT AS Text,
# MAGIC   fd.HSL AS AmountLC,
# MAGIC   fd.KSL AS AmountGC,
# MAGIC   fd.WSL AS AmountTC,
# MAGIC   fd.RHCUR AS LocalCurrency,
# MAGIC   fd.RKCUR AS GroupCurrency,
# MAGIC   fd.RTCUR AS TransactionCurrency,
# MAGIC   CASE
# MAGIC     WHEN er.ExchangeRate IS NOT NULL THEN (fd.HSL * er.ExchangeRate) - fd.KSL
# MAGIC     ELSE 0
# MAGIC   END AS GainLossGC,
# MAGIC   current_timestamp() AS CreatedAt,
# MAGIC   'Finance_Data_Processing_Job' AS CreatedBy
# MAGIC FROM finance_filtered_data fd
# MAGIC LEFT JOIN golden_views.entity e
# MAGIC   ON fd.RBUKRS = e.CompanyCode
# MAGIC LEFT JOIN golden_views.gl_account gl
# MAGIC   ON fd.RACCT = gl.SourceGLAccount
# MAGIC   AND fd.RBUKRS = gl.CompanyCode
# MAGIC LEFT JOIN golden_views.trading_partner tp
# MAGIC   ON fd.LIFNR = tp.VendorID
# MAGIC   OR fd.KUNNR = tp.CustomerID
# MAGIC LEFT JOIN s_shared.v_actual_exchange_rate_bpc er
# MAGIC   ON fd.RHCUR = er.FromCurrency
# MAGIC   AND fd.RKCUR = er.ToCurrency
# MAGIC   AND CONCAT(fd.RYEAR, LPAD(fd.POPER, 2, '0'), '01') = er.RateDate;

# COMMAND ----------

# MAGIC %python
# MAGIC # Log the completion of data transformation
# MAGIC try:
# MAGIC     transformed_data_count = spark.sql("SELECT COUNT(*) as count FROM finance_transformed_data").collect()[0]["count"]
# MAGIC     log_processing_step("Data Transformation", transformed_data_count)
# MAGIC except Exception as e:
# MAGIC     log_processing_step("Data Transformation", error=str(e))
# MAGIC     raise e

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Load transformed data into target Finance table
# MAGIC -- First, create the table if it doesn't exist
# MAGIC CREATE TABLE IF NOT EXISTS finance.finance_data (
# MAGIC   SourceDocumentID STRING,
# MAGIC   SourceFiscalYear STRING,
# MAGIC   SourcePeriod STRING,
# MAGIC   CompCode STRING,
# MAGIC   LegalEntity STRING,
# MAGIC   GoldenGLAcct STRING,
# MAGIC   SourceGLAcct STRING,
# MAGIC   CostCenter STRING,
# MAGIC   ProfitCenter STRING,
# MAGIC   FunctionalArea STRING,
# MAGIC   GoldenTradingPartner STRING,
# MAGIC   Assignment STRING,
# MAGIC   Text STRING,
# MAGIC   AmountLC DECIMAL(17,2),
# MAGIC   AmountGC DECIMAL(17,2),
# MAGIC   AmountTC DECIMAL(17,2),
# MAGIC   LocalCurrency STRING,
# MAGIC   GroupCurrency STRING,
# MAGIC   TransactionCurrency STRING,
# MAGIC   GainLossGC DECIMAL(17,2),
# MAGIC   CreatedAt TIMESTAMP,
# MAGIC   CreatedBy STRING,
# MAGIC   ProcessedDate DATE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert transformed data into the target table
# MAGIC INSERT INTO finance.finance_data
# MAGIC SELECT
# MAGIC   SourceDocumentID,
# MAGIC   SourceFiscalYear,
# MAGIC   SourcePeriod,
# MAGIC   CompCode,
# MAGIC   LegalEntity,
# MAGIC   GoldenGLAcct,
# MAGIC   SourceGLAcct,
# MAGIC   CostCenter,
# MAGIC   ProfitCenter,
# MAGIC   FunctionalArea,
# MAGIC   GoldenTradingPartner,
# MAGIC   Assignment,
# MAGIC   Text,
# MAGIC   AmountLC,
# MAGIC   AmountGC,
# MAGIC   AmountTC,
# MAGIC   LocalCurrency,
# MAGIC   GroupCurrency,
# MAGIC   TransactionCurrency,
# MAGIC   GainLossGC,
# MAGIC   CreatedAt,
# MAGIC   CreatedBy,
# MAGIC   current_date() AS ProcessedDate
# MAGIC FROM finance_transformed_data;

# COMMAND ----------

# MAGIC %python
# MAGIC # Log the completion of data loading
# MAGIC try:
# MAGIC     loaded_data_count = spark.sql("SELECT COUNT(*) as count FROM finance.finance_data WHERE ProcessedDate = current_date()").collect()[0]["count"]
# MAGIC     log_processing_step("Data Loading", loaded_data_count)
# MAGIC except Exception as e:
# MAGIC     log_processing_step("Data Loading", error=str(e))
# MAGIC     raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality Checks and Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for null values in critical columns
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_records,
# MAGIC   SUM(CASE WHEN SourceDocumentID IS NULL THEN 1 ELSE 0 END) AS null_document_id,
# MAGIC   SUM(CASE WHEN LegalEntity IS NULL THEN 1 ELSE 0 END) AS null_legal_entity,
# MAGIC   SUM(CASE WHEN GoldenGLAcct IS NULL THEN 1 ELSE 0 END) AS null_golden_gl_acct,
# MAGIC   SUM(CASE WHEN AmountLC IS NULL THEN 1 ELSE 0 END) AS null_amount_lc
# MAGIC FROM finance.finance_data
# MAGIC WHERE ProcessedDate = current_date();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for data consistency - total amounts should balance
# MAGIC SELECT
# MAGIC   SUM(AmountLC) AS total_amount_lc,
# MAGIC   SUM(AmountGC) AS total_amount_gc,
# MAGIC   SUM(GainLossGC) AS total_gain_loss_gc
# MAGIC FROM finance.finance_data
# MAGIC WHERE ProcessedDate = current_date();

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary and Audit

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate summary statistics for today's processed data
# MAGIC SELECT
# MAGIC   ProcessedDate,
# MAGIC   COUNT(*) AS total_records,
# MAGIC   COUNT(DISTINCT CompCode) AS distinct_company_codes,
# MAGIC   COUNT(DISTINCT LegalEntity) AS distinct_legal_entities,
# MAGIC   COUNT(DISTINCT GoldenGLAcct) AS distinct_gl_accounts,
# MAGIC   SUM(AmountLC) AS total_amount_lc,
# MAGIC   SUM(AmountGC) AS total_amount_gc
# MAGIC FROM finance.finance_data
# MAGIC WHERE ProcessedDate = current_date()
# MAGIC GROUP BY ProcessedDate;

# COMMAND ----------

# MAGIC %python
# MAGIC # Final logging of job completion
# MAGIC log_processing_step("Finance Data Processing Job Completed")
# MAGIC 
# MAGIC # Create audit entry
# MAGIC audit_data = {
# MAGIC     "job_name": "Finance_Data_Processing",
# MAGIC     "job_id": spark.conf.get("spark.databricks.job.id", "interactive"),
# MAGIC     "run_date": datetime.now().strftime("%Y-%m-%d"),
# MAGIC     "run_time": datetime.now().strftime("%H:%M:%S"),
# MAGIC     "status": "SUCCESS",
# MAGIC     "records_processed": spark.sql("SELECT COUNT(*) as count FROM finance.finance_data WHERE ProcessedDate = current_date()").collect()[0]["count"],
# MAGIC     "execution_time_seconds": None  # Would be calculated in a production job
# MAGIC }
# MAGIC 
# MAGIC # In a production environment, this would be written to an audit table
# MAGIC print(f"Job Audit Information: {audit_data}")