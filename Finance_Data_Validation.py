# Databricks notebook source
# MAGIC %md
# MAGIC # Finance Data Validation
# MAGIC 
# MAGIC This notebook performs validation checks on the finance data processing results to ensure data quality and completeness.

# COMMAND ----------

# MAGIC %run ../Shared/Config/Finance_Config

# COMMAND ----------

# Import required libraries
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
import logging

# COMMAND ----------

# Widget setup for parameters
dbutils.widgets.text("FiscalYear", "", "Fiscal Year")
dbutils.widgets.text("PostingPeriod", "", "Posting Period")

# Get parameter values
fiscal_year = dbutils.widgets.get("FiscalYear")
posting_period = dbutils.widgets.get("PostingPeriod")

# Validate parameters
if not fiscal_year or not posting_period:
    raise ValueError("FiscalYear and PostingPeriod parameters are required")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Completeness Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check record counts in source and target
# MAGIC WITH source_counts AS (
# MAGIC   SELECT 
# MAGIC     COUNT(*) as source_count
# MAGIC   FROM everest_ecc.FAGLFLEXA f
# MAGIC   INNER JOIN everest_ecc.BSEG b
# MAGIC   ON f.DOCNR = b.BELNR
# MAGIC   AND f.RBUKRS = b.BUKRS
# MAGIC   AND f.RYEAR = b.GJAHR
# MAGIC   WHERE f.RLDNR = '0L'
# MAGIC   AND f.RYEAR = '${FiscalYear}'
# MAGIC   AND b.XBILK = 'X'
# MAGIC ),
# MAGIC target_counts AS (
# MAGIC   SELECT 
# MAGIC     COUNT(*) as target_count
# MAGIC   FROM finance.financial_data
# MAGIC   WHERE FiscalYear = '${FiscalYear}'
# MAGIC   AND PostingPeriod = '${PostingPeriod}'
# MAGIC )
# MAGIC SELECT 
# MAGIC   s.source_count,
# MAGIC   t.target_count,
# MAGIC   CASE 
# MAGIC     WHEN s.source_count = t.target_count THEN 'PASS'
# MAGIC     ELSE 'FAIL'
# MAGIC   END as validation_result
# MAGIC FROM source_counts s, target_counts t;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for null values in key fields
# MAGIC SELECT
# MAGIC   'Null Check' as check_type,
# MAGIC   SUM(CASE WHEN CompanyCode IS NULL THEN 1 ELSE 0 END) as null_company_code,
# MAGIC   SUM(CASE WHEN GLAccount IS NULL THEN 1 ELSE 0 END) as null_gl_account,
# MAGIC   SUM(CASE WHEN DocumentNumber IS NULL THEN 1 ELSE 0 END) as null_document_number,
# MAGIC   SUM(CASE WHEN PostingDate IS NULL THEN 1 ELSE 0 END) as null_posting_date,
# MAGIC   SUM(CASE WHEN AmountLC IS NULL THEN 1 ELSE 0 END) as null_amount_lc,
# MAGIC   SUM(CASE WHEN AmountGC IS NULL THEN 1 ELSE 0 END) as null_amount_gc,
# MAGIC   COUNT(*) as total_records
# MAGIC FROM finance.financial_data
# MAGIC WHERE FiscalYear = '${FiscalYear}'
# MAGIC AND PostingPeriod = '${PostingPeriod}';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for unmapped reference data
# MAGIC SELECT
# MAGIC   'Reference Data Mapping Check' as check_type,
# MAGIC   SUM(CASE WHEN EntityName IS NULL THEN 1 ELSE 0 END) as unmapped_entity,
# MAGIC   SUM(CASE WHEN GLAccountName IS NULL THEN 1 ELSE 0 END) as unmapped_gl_account,
# MAGIC   SUM(CASE WHEN TradingPartner IS NOT NULL AND TradingPartnerName IS NULL THEN 1 ELSE 0 END) as unmapped_trading_partner,
# MAGIC   COUNT(*) as total_records
# MAGIC FROM finance.financial_data
# MAGIC WHERE FiscalYear = '${FiscalYear}'
# MAGIC AND PostingPeriod = '${PostingPeriod}';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for exchange rate application
# MAGIC SELECT
# MAGIC   'Exchange Rate Check' as check_type,
# MAGIC   SUM(CASE WHEN EntityCurrency != 'USD' AND ExchangeRate IS NULL THEN 1 ELSE 0 END) as missing_exchange_rate,
# MAGIC   SUM(CASE WHEN EntityCurrency != 'USD' AND ABS(AmountGC - (AmountLC * ExchangeRate)) > 0.01 THEN 1 ELSE 0 END) as incorrect_gc_calculation,
# MAGIC   COUNT(*) as total_records
# MAGIC FROM finance.financial_data
# MAGIC WHERE FiscalYear = '${FiscalYear}'
# MAGIC AND PostingPeriod = '${PostingPeriod}';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Balance Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if debits equal credits
# MAGIC WITH balance_check AS (
# MAGIC   SELECT
# MAGIC     CompanyCode,
# MAGIC     SUM(CASE WHEN PostingKey IN ('40', '50') THEN AmountLC ELSE 0 END) as debit_lc,
# MAGIC     SUM(CASE WHEN PostingKey IN ('31', '50') THEN AmountLC ELSE 0 END) as credit_lc,
# MAGIC     SUM(CASE WHEN PostingKey IN ('40', '50') THEN AmountGC ELSE 0 END) as debit_gc,
# MAGIC     SUM(CASE WHEN PostingKey IN ('31', '50') THEN AmountGC ELSE 0 END) as credit_gc
# MAGIC   FROM finance.financial_data
# MAGIC   WHERE FiscalYear = '${FiscalYear}'
# MAGIC   AND PostingPeriod = '${PostingPeriod}'
# MAGIC   GROUP BY CompanyCode
# MAGIC )
# MAGIC SELECT
# MAGIC   CompanyCode,
# MAGIC   debit_lc,
# MAGIC   credit_lc,
# MAGIC   ABS(debit_lc - credit_lc) as difference_lc,
# MAGIC   debit_gc,
# MAGIC   credit_gc,
# MAGIC   ABS(debit_gc - credit_gc) as difference_gc,
# MAGIC   CASE WHEN ABS(debit_lc - credit_lc) < 0.01 THEN 'BALANCED' ELSE 'UNBALANCED' END as balance_status_lc,
# MAGIC   CASE WHEN ABS(debit_gc - credit_gc) < 0.01 THEN 'BALANCED' ELSE 'UNBALANCED' END as balance_status_gc
# MAGIC FROM balance_check
# MAGIC ORDER BY difference_lc DESC, difference_gc DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate summary report
# MAGIC WITH company_summary AS (
# MAGIC   SELECT
# MAGIC     CompanyCode,
# MAGIC     EntityName,
# MAGIC     EntityCurrency,
# MAGIC     COUNT(*) as record_count,
# MAGIC     SUM(AmountLC) as total_amount_lc,
# MAGIC     SUM(AmountGC) as total_amount_gc
# MAGIC   FROM finance.financial_data
# MAGIC   WHERE FiscalYear = '${FiscalYear}'
# MAGIC   AND PostingPeriod = '${PostingPeriod}'
# MAGIC   GROUP BY CompanyCode, EntityName, EntityCurrency
# MAGIC )
# MAGIC SELECT
# MAGIC   CompanyCode,
# MAGIC   EntityName,
# MAGIC   EntityCurrency,
# MAGIC   record_count,
# MAGIC   total_amount_lc,
# MAGIC   total_amount_gc
# MAGIC FROM company_summary
# MAGIC ORDER BY record_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Results

# COMMAND ----------

# Check if all validations passed
validation_results = spark.sql(f"""
WITH validation_checks AS (
  -- Count check
  SELECT
    'Record Count Check' as check_name,
    CASE 
      WHEN (
        SELECT COUNT(*) FROM finance.financial_data
        WHERE FiscalYear = '{fiscal_year}'
        AND PostingPeriod = '{posting_period}'
      ) > 0 THEN 'PASS' 
      ELSE 'FAIL' 
    END as result
  
  UNION ALL
  
  -- Null check
  SELECT
    'Null Key Fields Check' as check_name,
    CASE 
      WHEN (
        SELECT SUM(
          CASE WHEN CompanyCode IS NULL OR 
                    GLAccount IS NULL OR 
                    DocumentNumber IS NULL OR 
                    PostingDate IS NULL OR 
                    AmountLC IS NULL
               THEN 1 ELSE 0 END
        ) 
        FROM finance.financial_data
        WHERE FiscalYear = '{fiscal_year}'
        AND PostingPeriod = '{posting_period}'
      ) = 0 THEN 'PASS' 
      ELSE 'FAIL' 
    END as result
    
  UNION ALL
  
  -- Balance check
  SELECT
    'Balance Check' as check_name,
    CASE 
      WHEN (
        SELECT COUNT(*)
        FROM (
          SELECT
            CompanyCode,
            ABS(SUM(CASE WHEN PostingKey IN ('40', '50') THEN AmountLC ELSE 0 END) - 
                SUM(CASE WHEN PostingKey IN ('31', '50') THEN AmountLC ELSE 0 END)) as diff
          FROM finance.financial_data
          WHERE FiscalYear = '{fiscal_year}'
          AND PostingPeriod = '{posting_period}'
          GROUP BY CompanyCode
          HAVING diff > 0.01
        )
      ) = 0 THEN 'PASS' 
      ELSE 'FAIL' 
    END as result
)
SELECT 
  check_name,
  result,
  CASE WHEN result = 'PASS' THEN '✅' ELSE '❌' END as status
FROM validation_checks
""")

display(validation_results)

# COMMAND ----------

# Final validation status
all_passed = validation_results.filter("result = 'FAIL'").count() == 0
print(f"Overall Validation Status: {'✅ PASSED' if all_passed else '❌ FAILED'}")