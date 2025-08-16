%sql
-- Validate gain/loss calculations
-- This query validates the gain/loss calculations for accuracy

SELECT 
  CompanyCode,
  LocalCurrency,
  TransactionCurrency,
  GroupCurrency,
  COUNT(*) AS RecordCount,
  SUM(AmountInLocalCurrency) AS TotalAmountLC,
  SUM(AmountInGroupCurrency) AS TotalAmountGC,
  SUM(AmountInTransactionCurrency) AS TotalAmountTC,
  SUM(GainLossLC) AS TotalGainLossLC,
  SUM(GainLossGC) AS TotalGainLossGC,
  SUM(GainLossTC) AS TotalGainLossTC,
  AVG(ExchangeRate) AS AvgExchangeRate
FROM 
  Finance.Finance_Data
WHERE 
  LocalCurrency != TransactionCurrency
  OR GroupCurrency != TransactionCurrency
GROUP BY 
  CompanyCode, LocalCurrency, TransactionCurrency, GroupCurrency
ORDER BY 
  CompanyCode;