%sql
-- Validate data quality
-- This query checks for potential data quality issues

SELECT 
  SUM(CASE WHEN Client IS NULL THEN 1 ELSE 0 END) AS NullClientCount,
  SUM(CASE WHEN CompanyCode IS NULL THEN 1 ELSE 0 END) AS NullCompanyCodeCount,
  SUM(CASE WHEN DocumentNumber IS NULL THEN 1 ELSE 0 END) AS NullDocumentNumberCount,
  SUM(CASE WHEN GLAccount IS NULL THEN 1 ELSE 0 END) AS NullGLAccountCount,
  SUM(CASE WHEN LocalCurrency IS NULL THEN 1 ELSE 0 END) AS NullLocalCurrencyCount,
  SUM(CASE WHEN GroupCurrency IS NULL THEN 1 ELSE 0 END) AS NullGroupCurrencyCount,
  SUM(CASE WHEN TransactionCurrency IS NULL THEN 1 ELSE 0 END) AS NullTransactionCurrencyCount,
  SUM(CASE WHEN ExchangeRate IS NULL THEN 1 ELSE 0 END) AS NullExchangeRateCount,
  SUM(CASE WHEN EntityID IS NULL THEN 1 ELSE 0 END) AS NullEntityIDCount
FROM 
  Finance.Finance_Data;