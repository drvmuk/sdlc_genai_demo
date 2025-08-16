%sql
-- Validate joined data
-- This query checks the result of joining FAGLFLEXA and BSEG

SELECT 
  COUNT(*) AS JoinedRecordCount,
  COUNT(DISTINCT CompanyCode) AS CompanyCodeCount,
  COUNT(DISTINCT FiscalYear) AS FiscalYearCount,
  SUM(CASE WHEN CompanyCode LIKE '8%' THEN 1 ELSE 0 END) AS CompanyCode8Count,
  SUM(CASE WHEN XBILK = 'X' THEN 1 ELSE 0 END) AS XBILKCount
FROM 
  v_finance_joined;