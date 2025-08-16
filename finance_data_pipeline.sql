%sql
-- Step 1: Create a temporary view of FAGLFLEXA data with the required filter
CREATE OR REPLACE TEMPORARY VIEW v_faglflexa AS
SELECT 
  RBUKRS AS company_code,
  RYEAR AS fiscal_year,
  POPER AS posting_period,
  DOCNR AS document_number,
  RLDNR AS ledger,
  RACCT AS gl_account,
  RCNTR AS cost_center,
  PRCTR AS profit_center,
  RFAREA AS functional_area,
  RBUSA AS business_area,
  KOKRS AS controlling_area,
  RASSC AS trading_partner,
  DRCRK AS debit_credit_indicator,
  HSLVT AS amount_in_transaction_currency,
  RHCUR AS transaction_currency,
  HSL AS amount_in_company_code_currency,
  RHCUR AS company_code_currency,
  RVERS AS version,
  GJAHR AS fiscal_year_reference,
  AUGDT AS clearing_date,
  AUGBL AS clearing_document
FROM ecc_everest.FAGLFLEXA
WHERE RLDNR = '0L';

%sql
-- Step 2: Create a temporary view of BSEG data with the required filter
CREATE OR REPLACE TEMPORARY VIEW v_bseg AS
SELECT 
  BUKRS AS company_code,
  GJAHR AS fiscal_year,
  BELNR AS document_number,
  BUZEI AS line_item,
  XBILK AS ledger_indicator,
  BUDAT AS posting_date,
  BLDAT AS document_date,
  MONAT AS posting_period,
  BSCHL AS posting_key,
  SHKZG AS debit_credit_indicator,
  GSBER AS business_area,
  HKONT AS gl_account,
  PRCTR AS profit_center,
  KOSTL AS cost_center,
  ZUONR AS assignment_number,
  SGTXT AS line_item_text,
  DMBTR AS amount_in_local_currency,
  WRBTR AS amount_in_document_currency,
  WAERS AS document_currency
FROM ecc_everest.BSEG
WHERE XBILK = 'X';

%sql
-- Step 3: Join FAGLFLEXA and BSEG and apply filters based on FY and Posting Period parameters
CREATE OR REPLACE TEMPORARY VIEW v_finance_base AS
SELECT 
  f.company_code,
  f.fiscal_year,
  f.posting_period,
  f.document_number,
  f.gl_account,
  f.cost_center,
  f.profit_center,
  f.functional_area,
  f.business_area,
  f.controlling_area,
  f.trading_partner,
  f.debit_credit_indicator,
  f.amount_in_transaction_currency,
  f.transaction_currency,
  f.amount_in_company_code_currency,
  f.company_code_currency,
  b.posting_date,
  b.document_date,
  b.posting_key,
  b.assignment_number,
  b.line_item_text,
  b.line_item
FROM v_faglflexa f
JOIN v_bseg b
  ON f.document_number = b.document_number
  AND f.company_code = b.company_code
  AND f.fiscal_year = b.fiscal_year
WHERE f.fiscal_year = ${FY}
  AND f.posting_period <= ${PostingPeriod};

%sql
-- Step 4: Transform data and join with golden views for mapping
CREATE OR REPLACE TEMPORARY VIEW v_finance_transformed AS
SELECT
  fb.company_code,
  e.entity_name AS entity,
  fb.fiscal_year,
  fb.posting_period,
  fb.document_number,
  fb.posting_date,
  fb.document_date,
  gl.gl_account_name AS gl_account_name,
  gl.gl_account_category AS gl_account_category,
  gl.gl_account_type AS gl_account_type,
  fb.gl_account,
  fb.cost_center,
  fb.profit_center,
  fb.functional_area,
  fb.business_area,
  fb.controlling_area,
  tp.trading_partner_name AS trading_partner_name,
  fb.trading_partner,
  fb.posting_key,
  fb.assignment_number,
  fb.line_item_text,
  fb.line_item,
  fb.debit_credit_indicator,
  CASE 
    WHEN fb.debit_credit_indicator = 'S' THEN fb.amount_in_transaction_currency
    ELSE -1 * fb.amount_in_transaction_currency
  END AS amount_in_transaction_currency,
  fb.transaction_currency,
  CASE 
    WHEN fb.debit_credit_indicator = 'S' THEN fb.amount_in_company_code_currency
    ELSE -1 * fb.amount_in_company_code_currency
  END AS amount_in_company_code_currency,
  fb.company_code_currency,
  -- Convert to USD using exchange rates
  CASE 
    WHEN fb.debit_credit_indicator = 'S' THEN fb.amount_in_company_code_currency * er.exchange_rate
    ELSE -1 * fb.amount_in_company_code_currency * er.exchange_rate
  END AS amount_in_usd,
  'USD' AS usd_currency,
  CURRENT_TIMESTAMP() AS load_timestamp,
  NULL AS offset_account -- Will be populated in Step 5
FROM v_finance_base fb
LEFT JOIN golden.entity_golden_view e
  ON fb.company_code = e.company_code
LEFT JOIN golden.gl_golden_view gl
  ON fb.gl_account = gl.gl_account_number
LEFT JOIN golden.trading_partner_golden_view tp
  ON fb.trading_partner = tp.trading_partner_code
LEFT JOIN s_shared.v_actual_exchange_rate_bpc er
  ON fb.company_code_currency = er.from_currency
  AND fb.posting_date = er.rate_date
  AND er.to_currency = 'USD';

%sql
-- Step 5: Determine offset account for realized and unrealized accounts
CREATE OR REPLACE TEMPORARY VIEW v_offset_accounts AS
WITH realized_unrealized_accounts AS (
  SELECT
    document_number,
    company_code,
    fiscal_year,
    posting_period,
    line_item,
    gl_account,
    amount_in_company_code_currency,
    gl_account_category
  FROM v_finance_transformed
  WHERE gl_account_category IN ('Realized', 'Unrealized')
),
document_accounts AS (
  SELECT
    document_number,
    company_code,
    fiscal_year,
    posting_period,
    gl_account AS offset_account,
    amount_in_company_code_currency
  FROM v_finance_transformed
  WHERE gl_account_category NOT IN ('Realized', 'Unrealized')
)
SELECT
  r.document_number,
  r.company_code,
  r.fiscal_year,
  r.posting_period,
  r.line_item,
  r.gl_account,
  d.offset_account
FROM realized_unrealized_accounts r
JOIN document_accounts d
  ON r.document_number = d.document_number
  AND r.company_code = d.company_code
  AND r.fiscal_year = d.fiscal_year
  AND r.posting_period = d.posting_period
  -- Match by opposite amounts (debits match with credits)
  AND r.amount_in_company_code_currency = -1 * d.amount_in_company_code_currency;

%sql
-- Step 6: Create the final Finance table with offset account information
CREATE TABLE IF NOT EXISTS finance.finance (
  company_code STRING,
  entity STRING,
  fiscal_year INT,
  posting_period INT,
  document_number STRING,
  posting_date DATE,
  document_date DATE,
  gl_account_name STRING,
  gl_account_category STRING,
  gl_account_type STRING,
  gl_account STRING,
  cost_center STRING,
  profit_center STRING,
  functional_area STRING,
  business_area STRING,
  controlling_area STRING,
  trading_partner_name STRING,
  trading_partner STRING,
  posting_key STRING,
  assignment_number STRING,
  line_item_text STRING,
  line_item STRING,
  debit_credit_indicator STRING,
  amount_in_transaction_currency DECIMAL(17,2),
  transaction_currency STRING,
  amount_in_company_code_currency DECIMAL(17,2),
  company_code_currency STRING,
  amount_in_usd DECIMAL(17,2),
  usd_currency STRING,
  offset_account STRING,
  load_timestamp TIMESTAMP
)
USING PARQUET
PARTITIONED BY (fiscal_year, posting_period);

%sql
-- Insert transformed data into the Finance table with offset account information
INSERT OVERWRITE TABLE finance.finance
SELECT
  t.company_code,
  t.entity,
  t.fiscal_year,
  t.posting_period,
  t.document_number,
  t.posting_date,
  t.document_date,
  t.gl_account_name,
  t.gl_account_category,
  t.gl_account_type,
  t.gl_account,
  t.cost_center,
  t.profit_center,
  t.functional_area,
  t.business_area,
  t.controlling_area,
  t.trading_partner_name,
  t.trading_partner,
  t.posting_key,
  t.assignment_number,
  t.line_item_text,
  t.line_item,
  t.debit_credit_indicator,
  t.amount_in_transaction_currency,
  t.transaction_currency,
  t.amount_in_company_code_currency,
  t.company_code_currency,
  t.amount_in_usd,
  t.usd_currency,
  COALESCE(o.offset_account, t.offset_account) AS offset_account,
  t.load_timestamp
FROM v_finance_transformed t
LEFT JOIN v_offset_accounts o
  ON t.document_number = o.document_number
  AND t.company_code = o.company_code
  AND t.fiscal_year = o.fiscal_year
  AND t.posting_period = o.posting_period
  AND t.line_item = o.line_item
  AND t.gl_account = o.gl_account;

%sql
-- Create a log table to track job execution and errors
CREATE TABLE IF NOT EXISTS finance.job_log (
  job_id STRING,
  job_name STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status STRING,
  records_processed BIGINT,
  error_message STRING,
  fiscal_year INT,
  posting_period INT
)
USING PARQUET;

%sql
-- Log successful job execution
INSERT INTO finance.job_log
SELECT
  uuid() AS job_id,
  'Finance Data Pipeline' AS job_name,
  CURRENT_TIMESTAMP() AS start_time,
  CURRENT_TIMESTAMP() AS end_time,
  'SUCCESS' AS status,
  (SELECT COUNT(*) FROM finance.finance WHERE fiscal_year = ${FY} AND posting_period <= ${PostingPeriod}) AS records_processed,
  NULL AS error_message,
  ${FY} AS fiscal_year,
  ${PostingPeriod} AS posting_period;

%sql
-- Create a view for data validation and reporting
CREATE OR REPLACE VIEW finance.v_finance_summary AS
SELECT
  fiscal_year,
  posting_period,
  company_code,
  entity,
  gl_account_category,
  COUNT(*) AS transaction_count,
  SUM(amount_in_usd) AS total_amount_usd,
  MIN(posting_date) AS min_posting_date,
  MAX(posting_date) AS max_posting_date,
  MAX(load_timestamp) AS latest_load_timestamp
FROM finance.finance
GROUP BY
  fiscal_year,
  posting_period,
  company_code,
  entity,
  gl_account_category;