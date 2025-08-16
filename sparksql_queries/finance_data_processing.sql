%sql
-- Create temporary view for FAGLFLEXA and BSEG joined data
-- This forms the base dataset for our transformations
CREATE OR REPLACE TEMPORARY VIEW finance_base_data AS
SELECT 
  f.DOCNR,
  f.RBUKRS,
  f.RACCT,
  f.RASSC,
  f.HSL,
  f.TSL,
  f.RTCUR,
  b.BELNR,
  b.BUZEI,
  b.AUGDT,
  b.AUGBL
FROM 
  FAGLFLEXA f
JOIN 
  BSEG b 
ON 
  f.DOCNR = b.DOCNR
WHERE 
  f.RBUKRS NOT LIKE '8%';

%sql
-- Create view for entity mapping
CREATE OR REPLACE TEMPORARY VIEW entity_mapping AS
SELECT 
  company_code,
  golden_entity,
  local_currency
FROM 
  entity_golden_view;

%sql
-- Create view for GL account mapping
CREATE OR REPLACE TEMPORARY VIEW gl_mapping AS
SELECT 
  source_gl_account,
  golden_gl_account
FROM 
  golden_gl_view;

%sql
-- Create view for trading partner mapping
CREATE OR REPLACE TEMPORARY VIEW trading_partner_mapping AS
SELECT 
  source_trading_partner,
  golden_trading_partner
FROM 
  golden_trading_partner_view;

%sql
-- Create view for exchange rates
CREATE OR REPLACE TEMPORARY VIEW exchange_rates AS
SELECT 
  from_currency,
  to_currency,
  fiscal_year,
  period,
  exchange_rate
FROM 
  s_shared.v_actual_exchange_rate_bpc;

%sql
-- Main transformation query to process finance data
-- Parameters: ${FY} for fiscal year and ${PostingPeriod} for posting period
CREATE OR REPLACE TEMPORARY VIEW finance_transformed_data AS
SELECT
  -- Derive fiscal year and posting period from parameters
  '${FY}' AS FiscalYear,
  '${PostingPeriod}' AS PostingPeriod,
  
  -- Document information
  f.DOCNR AS DocumentNumber,
  
  -- Company code and entity information
  f.RBUKRS AS CompCode,
  e.golden_entity AS LegalEntity,
  
  -- GL account information
  f.RACCT AS GLAccount,
  gl.golden_gl_account AS GoldenGLAcct,
  
  -- Trading partner information
  f.RASSC AS TradingPartner,
  tp.golden_trading_partner AS GoldenTradingPartner,
  
  -- Currency and amount information
  f.HSL AS GainLossLC,
  e.local_currency AS LocalCurrency,
  f.TSL AS GainLossTC,
  f.RTCUR AS TransactionCurrency,
  
  -- Calculate GainLossGC using exchange rates
  CASE
    WHEN er.exchange_rate IS NOT NULL THEN f.HSL * er.exchange_rate
    ELSE NULL
  END AS GainLossGC,
  
  -- Derive OffsetAccount based on business logic
  CASE
    -- Logic for Realized accounts
    WHEN b.AUGDT IS NOT NULL AND b.AUGBL IS NOT NULL THEN 
      CASE
        WHEN f.RACCT BETWEEN '310000' AND '319999' THEN '310900'
        WHEN f.RACCT BETWEEN '320000' AND '329999' THEN '320900'
        ELSE f.RACCT
      END
    -- Logic for Unrealized accounts
    ELSE
      CASE
        WHEN f.RACCT BETWEEN '310000' AND '319999' THEN '310800'
        WHEN f.RACCT BETWEEN '320000' AND '329999' THEN '320800'
        ELSE f.RACCT
      END
  END AS OffsetAccount,
  
  -- Derive GoldenOffsetAccount
  gl_offset.golden_gl_account AS GoldenOffsetAccount,
  
  -- Additional metadata
  current_timestamp() AS ProcessedTimestamp,
  'Finance Data Processing Job' AS ProcessedBy
FROM 
  finance_base_data f
LEFT JOIN 
  entity_mapping e ON f.RBUKRS = e.company_code
LEFT JOIN 
  gl_mapping gl ON f.RACCT = gl.source_gl_account
LEFT JOIN 
  trading_partner_mapping tp ON f.RASSC = tp.source_trading_partner
LEFT JOIN 
  exchange_rates er ON e.local_currency = er.from_currency 
                    AND 'USD' = er.to_currency 
                    AND '${FY}' = er.fiscal_year 
                    AND '${PostingPeriod}' = er.period
LEFT JOIN
  BSEG b ON f.DOCNR = b.DOCNR
LEFT JOIN
  gl_mapping gl_offset ON 
    CASE
      WHEN b.AUGDT IS NOT NULL AND b.AUGBL IS NOT NULL THEN 
        CASE
          WHEN f.RACCT BETWEEN '310000' AND '319999' THEN '310900'
          WHEN f.RACCT BETWEEN '320000' AND '329999' THEN '320900'
          ELSE f.RACCT
        END
      ELSE
        CASE
          WHEN f.RACCT BETWEEN '310000' AND '319999' THEN '310800'
          WHEN f.RACCT BETWEEN '320000' AND '329999' THEN '320800'
          ELSE f.RACCT
        END
    END = gl_offset.source_gl_account;

%sql
-- Insert transformed data into target Finance table
INSERT INTO Finance
SELECT
  FiscalYear,
  PostingPeriod,
  DocumentNumber,
  CompCode,
  LegalEntity,
  GLAccount,
  GoldenGLAcct,
  TradingPartner,
  GoldenTradingPartner,
  GainLossLC,
  LocalCurrency,
  GainLossTC,
  TransactionCurrency,
  GainLossGC,
  OffsetAccount,
  GoldenOffsetAccount,
  ProcessedTimestamp,
  ProcessedBy
FROM
  finance_transformed_data;

%sql
-- Verify data was loaded correctly
SELECT 
  COUNT(*) AS records_loaded,
  MIN(ProcessedTimestamp) AS earliest_processed,
  MAX(ProcessedTimestamp) AS latest_processed
FROM 
  Finance
WHERE 
  FiscalYear = '${FY}' 
  AND PostingPeriod = '${PostingPeriod}';

%sql
-- Log job execution status
CREATE TABLE IF NOT EXISTS job_execution_log (
  job_name STRING,
  execution_timestamp TIMESTAMP,
  parameters STRING,
  records_processed BIGINT,
  status STRING,
  error_message STRING
);

%sql
-- Insert job execution log entry
INSERT INTO job_execution_log
SELECT
  'Finance Data Processing Job' AS job_name,
  current_timestamp() AS execution_timestamp,
  concat('FY=', '${FY}', ', PostingPeriod=', '${PostingPeriod}') AS parameters,
  (SELECT COUNT(*) FROM finance_transformed_data) AS records_processed,
  'SUCCESS' AS status,
  NULL AS error_message;