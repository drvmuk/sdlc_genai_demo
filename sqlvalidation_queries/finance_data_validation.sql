%sql
-- Validation query 1: Check if source tables exist and have data
SELECT COUNT(*) AS faglflexa_count 
FROM FAGLFLEXA;

%sql
-- Validation query 2: Check if BSEG table exists and has data
SELECT COUNT(*) AS bseg_count 
FROM BSEG;

%sql
-- Validation query 3: Verify entity golden view has data
SELECT COUNT(*) AS entity_golden_view_count 
FROM entity_golden_view;

%sql
-- Validation query 4: Verify Golden GL view has data
SELECT COUNT(*) AS golden_gl_view_count 
FROM golden_gl_view;

%sql
-- Validation query 5: Verify Golden Trading Partner view has data
SELECT COUNT(*) AS golden_trading_partner_view_count 
FROM golden_trading_partner_view;

%sql
-- Validation query 6: Check if revenue entity mapping view has data
SELECT COUNT(*) AS revenue_entity_count 
FROM s_shared.v_revenue_entity_bpc;

%sql
-- Validation query 7: Check if exchange rate view has data
SELECT COUNT(*) AS exchange_rate_count 
FROM s_shared.v_actual_exchange_rate_bpc;

%sql
-- Validation query 8: Check for company codes starting with '8' that should be excluded
SELECT DISTINCT RBUKRS 
FROM FAGLFLEXA 
WHERE RBUKRS LIKE '8%';

%sql
-- Validation query 9: Check for null values in key join columns
SELECT COUNT(*) AS null_join_keys 
FROM FAGLFLEXA f
LEFT JOIN BSEG b ON f.DOCNR = b.DOCNR
WHERE f.DOCNR IS NULL OR b.DOCNR IS NULL;

%sql
-- Validation query 10: Verify target Finance table structure
DESCRIBE Finance;