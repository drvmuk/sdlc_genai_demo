-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Finance Data Processing Job
-- MAGIC 
-- MAGIC **Technical Requirement ID:** TR-FIN-001  
-- MAGIC **Related Functional Requirement(s):** FR-FIN-001  
-- MAGIC **Objective:** Implement a data processing job to load finance data from Everest ECC into the Finance table by joining and transforming data from FAGLFLEXA and BSEG tables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Stage 1: Retrieve data from FAGLFLEXA and BSEG tables

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Create temporary view for FAGLFLEXA with initial filtering
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_faglflexa_filtered AS
-- MAGIC SELECT *
-- MAGIC FROM everest_ecc.FAGLFLEXA
-- MAGIC WHERE RLDNR = '0L' 
-- MAGIC   AND XBILK = 'X'
-- MAGIC   AND NOT RBUKRS LIKE '8%';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Create temporary view for BSEG with relevant columns
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_bseg_filtered AS
-- MAGIC SELECT 
-- MAGIC   BELNR,
-- MAGIC   BUKRS,
-- MAGIC   GJAHR,
-- MAGIC   BUZEI,
-- MAGIC   HKONT,
-- MAGIC   AUGDT,
-- MAGIC   AUGBL,
-- MAGIC   KOART,
-- MAGIC   UMSKZ,
-- MAGIC   MWSKZ,
-- MAGIC   BSCHL
-- MAGIC FROM everest_ecc.BSEG;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Stage 2: Apply filters and transformations - Join FAGLFLEXA and BSEG tables

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Join FAGLFLEXA and BSEG tables and apply initial transformations
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_finance_base AS
-- MAGIC SELECT
-- MAGIC   f.RBUKRS AS CompanyCode,
-- MAGIC   f.DOCNR AS DocumentNumber,
-- MAGIC   f.RYEAR AS FiscalYear,
-- MAGIC   f.POPER AS Period,
-- MAGIC   f.RACCT AS GLAccount,
-- MAGIC   f.RCNTR AS CostCenter,
-- MAGIC   f.DRCRK AS DebitCreditIndicator,
-- MAGIC   f.HSL AS AmountInLocalCurrency,
-- MAGIC   f.KSL AS AmountInGroupCurrency,
-- MAGIC   f.OSL AS AmountInTransactionCurrency,
-- MAGIC   f.RTCUR AS TransactionCurrency,
-- MAGIC   f.RUNIT AS BusinessArea,
-- MAGIC   f.TSLVT AS TaxCode,
-- MAGIC   f.BUDAT AS PostingDate,
-- MAGIC   f.BLDAT AS DocumentDate,
-- MAGIC   b.BUZEI AS LineItem,
-- MAGIC   b.AUGDT AS ClearingDate,
-- MAGIC   b.AUGBL AS ClearingDocument,
-- MAGIC   b.KOART AS AccountType,
-- MAGIC   b.UMSKZ AS SpecialGLIndicator,
-- MAGIC   b.MWSKZ AS TaxIndicator,
-- MAGIC   b.BSCHL AS PostingKey
-- MAGIC FROM vw_faglflexa_filtered f
-- MAGIC INNER JOIN vw_bseg_filtered b
-- MAGIC   ON f.DOCNR = b.BELNR
-- MAGIC   AND f.RBUKRS = b.BUKRS
-- MAGIC   AND f.RYEAR = b.GJAHR;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Stage 3: Apply Golden View mappings and calculate GainLossGC and GainLossLC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Map Company Code to Golden Entity and apply additional transformations
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_finance_transformed AS
-- MAGIC SELECT
-- MAGIC   fb.*,
-- MAGIC   eg.GoldenEntityID,
-- MAGIC   eg.GoldenEntityName,
-- MAGIC   gl.GoldenGLAccountID,
-- MAGIC   gl.GoldenGLAccountName,
-- MAGIC   tp.GoldenTradingPartnerID,
-- MAGIC   tp.GoldenTradingPartnerName,
-- MAGIC   -- Calculate GainLossLC
-- MAGIC   CASE 
-- MAGIC     WHEN er.LocalCurrencyCode != er.GroupCurrencyCode THEN
-- MAGIC       fb.AmountInLocalCurrency - (fb.AmountInTransactionCurrency * er.LocalToTransactionRate)
-- MAGIC     ELSE 0
-- MAGIC   END AS GainLossLC,
-- MAGIC   -- Calculate GainLossGC
-- MAGIC   CASE 
-- MAGIC     WHEN er.TransactionCurrencyCode != er.GroupCurrencyCode THEN
-- MAGIC       fb.AmountInGroupCurrency - (fb.AmountInTransactionCurrency * er.GroupToTransactionRate)
-- MAGIC     ELSE 0
-- MAGIC   END AS GainLossGC
-- MAGIC FROM vw_finance_base fb
-- MAGIC LEFT JOIN golden_views.entity_golden_view eg ON fb.CompanyCode = eg.SourceCompanyCode
-- MAGIC LEFT JOIN golden_views.gl_golden_view gl ON fb.GLAccount = gl.SourceGLAccount
-- MAGIC LEFT JOIN golden_views.trading_partner_golden_view tp ON fb.CompanyCode = tp.SourceCompanyCode
-- MAGIC LEFT JOIN s_shared.v_actual_exchange_rate_bpc er 
-- MAGIC   ON fb.TransactionCurrency = er.TransactionCurrencyCode
-- MAGIC   AND fb.PostingDate = er.RateDate;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Stage 4: Calculate GainLossTC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Calculate GainLossTC and prepare final dataset
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_finance_with_gainloss AS
-- MAGIC SELECT
-- MAGIC   ft.*,
-- MAGIC   -- Calculate GainLossTC based on GainLossGC and GainLossLC
-- MAGIC   CASE 
-- MAGIC     WHEN ft.GainLossGC != 0 OR ft.GainLossLC != 0 THEN
-- MAGIC       ft.GainLossGC + ft.GainLossLC
-- MAGIC     ELSE 0
-- MAGIC   END AS GainLossTC
-- MAGIC FROM vw_finance_transformed ft;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Stage 5: Determine Offset Account

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Determine Offset Account based on realized and unrealized accounts
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_finance_with_offset AS
-- MAGIC SELECT
-- MAGIC   fgl.*,
-- MAGIC   CASE
-- MAGIC     -- Check if account is in realized accounts list
-- MAGIC     WHEN ru.AccountType = 'Realized' AND fgl.ClearingDocument IS NOT NULL THEN
-- MAGIC       (SELECT OffsetAccount FROM golden_views.v_realized_unrealized_glaccts 
-- MAGIC        WHERE AccountType = 'Realized' AND GLAccount = fgl.GLAccount)
-- MAGIC     -- Check if account is in unrealized accounts list
-- MAGIC     WHEN ru.AccountType = 'Unrealized' THEN
-- MAGIC       (SELECT OffsetAccount FROM golden_views.v_realized_unrealized_glaccts 
-- MAGIC        WHERE AccountType = 'Unrealized' AND GLAccount = fgl.GLAccount)
-- MAGIC     -- Default offset account from BSEG if available
-- MAGIC     WHEN b.HKONT IS NOT NULL THEN b.HKONT
-- MAGIC     ELSE NULL
-- MAGIC   END AS OffsetAccount
-- MAGIC FROM vw_finance_with_gainloss fgl
-- MAGIC LEFT JOIN golden_views.v_realized_unrealized_glaccts ru ON fgl.GLAccount = ru.GLAccount
-- MAGIC LEFT JOIN everest_ecc.BSEG b 
-- MAGIC   ON fgl.DocumentNumber = b.BELNR
-- MAGIC   AND fgl.CompanyCode = b.BUKRS
-- MAGIC   AND fgl.FiscalYear = b.GJAHR
-- MAGIC   AND b.BUZEI != fgl.LineItem;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Stage 6: Load data into Finance table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Insert the final transformed data into the Finance table
-- MAGIC INSERT INTO finance.finance_table
-- MAGIC SELECT
-- MAGIC   GoldenEntityID,
-- MAGIC   GoldenEntityName,
-- MAGIC   CompanyCode,
-- MAGIC   DocumentNumber,
-- MAGIC   FiscalYear,
-- MAGIC   Period,
-- MAGIC   GoldenGLAccountID,
-- MAGIC   GoldenGLAccountName,
-- MAGIC   GLAccount,
-- MAGIC   CostCenter,
-- MAGIC   DebitCreditIndicator,
-- MAGIC   AmountInLocalCurrency,
-- MAGIC   AmountInGroupCurrency,
-- MAGIC   AmountInTransactionCurrency,
-- MAGIC   TransactionCurrency,
-- MAGIC   BusinessArea,
-- MAGIC   TaxCode,
-- MAGIC   PostingDate,
-- MAGIC   DocumentDate,
-- MAGIC   LineItem,
-- MAGIC   ClearingDate,
-- MAGIC   ClearingDocument,
-- MAGIC   AccountType,
-- MAGIC   SpecialGLIndicator,
-- MAGIC   TaxIndicator,
-- MAGIC   PostingKey,
-- MAGIC   GainLossLC,
-- MAGIC   GainLossGC,
-- MAGIC   GainLossTC,
-- MAGIC   OffsetAccount,
-- MAGIC   GoldenTradingPartnerID,
-- MAGIC   GoldenTradingPartnerName,
-- MAGIC   current_timestamp() AS LoadTimestamp
-- MAGIC FROM vw_finance_with_offset;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Error Handling and Logging

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Error handling and logging
-- MAGIC import logging
-- MAGIC from datetime import datetime
-- MAGIC 
-- MAGIC # Configure logging
-- MAGIC log_path = f"/dbfs/logs/finance_data_processing/{datetime.now().strftime('%Y-%m-%d')}/processing.log"
-- MAGIC logging.basicConfig(
-- MAGIC     filename=log_path,
-- MAGIC     level=logging.INFO,
-- MAGIC     format='%(asctime)s - %(levelname)s - %(message)s'
-- MAGIC )
-- MAGIC 
-- MAGIC try:
-- MAGIC     # Log successful completion
-- MAGIC     logging.info("Finance data processing job completed successfully")
-- MAGIC     
-- MAGIC     # Get record counts for validation
-- MAGIC     source_count = spark.sql("SELECT COUNT(*) AS count FROM vw_faglflexa_filtered").collect()[0]["count"]
-- MAGIC     target_count = spark.sql("SELECT COUNT(*) AS count FROM vw_finance_with_offset").collect()[0]["count"]
-- MAGIC     
-- MAGIC     logging.info(f"Source record count: {source_count}")
-- MAGIC     logging.info(f"Target record count: {target_count}")
-- MAGIC     
-- MAGIC     # Additional validation checks
-- MAGIC     null_entities = spark.sql("SELECT COUNT(*) AS count FROM vw_finance_with_offset WHERE GoldenEntityID IS NULL").collect()[0]["count"]
-- MAGIC     if null_entities > 0:
-- MAGIC         logging.warning(f"Found {null_entities} records with null GoldenEntityID")
-- MAGIC     
-- MAGIC     # Check for data quality issues
-- MAGIC     duplicate_check = spark.sql("""
-- MAGIC         SELECT COUNT(*) AS count FROM (
-- MAGIC             SELECT GoldenEntityID, DocumentNumber, FiscalYear, LineItem, COUNT(*) as cnt
-- MAGIC             FROM vw_finance_with_offset
-- MAGIC             GROUP BY GoldenEntityID, DocumentNumber, FiscalYear, LineItem
-- MAGIC             HAVING COUNT(*) > 1
-- MAGIC         )
-- MAGIC     """).collect()[0]["count"]
-- MAGIC     
-- MAGIC     if duplicate_check > 0:
-- MAGIC         logging.warning(f"Found {duplicate_check} potential duplicate records")
-- MAGIC         
-- MAGIC except Exception as e:
-- MAGIC     # Log error and send notification
-- MAGIC     error_message = f"Finance data processing job failed: {str(e)}"
-- MAGIC     logging.error(error_message)
-- MAGIC     
-- MAGIC     # Send notification (placeholder for actual notification mechanism)
-- MAGIC     dbutils.notebook.exit(error_message)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Job Summary and Validation

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Validate data load and provide summary
-- MAGIC SELECT 
-- MAGIC   'Record Count' AS Metric,
-- MAGIC   COUNT(*) AS Value
-- MAGIC FROM finance.finance_table
-- MAGIC WHERE LoadTimestamp >= (SELECT MAX(LoadTimestamp) FROM finance.finance_table)
-- MAGIC 
-- MAGIC UNION ALL
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   'Distinct Companies' AS Metric,
-- MAGIC   COUNT(DISTINCT CompanyCode) AS Value
-- MAGIC FROM finance.finance_table
-- MAGIC WHERE LoadTimestamp >= (SELECT MAX(LoadTimestamp) FROM finance.finance_table)
-- MAGIC 
-- MAGIC UNION ALL
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   'Null GoldenEntityID Count' AS Metric,
-- MAGIC   SUM(CASE WHEN GoldenEntityID IS NULL THEN 1 ELSE 0 END) AS Value
-- MAGIC FROM finance.finance_table
-- MAGIC WHERE LoadTimestamp >= (SELECT MAX(LoadTimestamp) FROM finance.finance_table)
-- MAGIC 
-- MAGIC UNION ALL
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   'Null OffsetAccount Count' AS Metric,
-- MAGIC   SUM(CASE WHEN OffsetAccount IS NULL THEN 1 ELSE 0 END) AS Value
-- MAGIC FROM finance.finance_table
-- MAGIC WHERE LoadTimestamp >= (SELECT MAX(LoadTimestamp) FROM finance.finance_table);