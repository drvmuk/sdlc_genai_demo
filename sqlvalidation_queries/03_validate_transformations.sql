%sql
-- Validate transformations
-- This query validates the transformations and calculations

SELECT 
  COUNT(*) AS TotalRecords,
  COUNT(DISTINCT EntityID) AS EntityCount,
  COUNT(CASE WHEN ExchangeRate IS NULL THEN 1 END) AS MissingExchangeRateCount,
  SUM(GainLossLC) AS TotalGainLossLC,
  SUM(GainLossGC) AS TotalGainLossGC,
  SUM(GainLossTC) AS TotalGainLossTC,
  COUNT(CASE WHEN OffsetAccount IS NULL THEN 1 END) AS MissingOffsetAccountCount,
  COUNT(CASE WHEN GoldenOffsetAccount IS NULL THEN 1 END) AS MissingGoldenOffsetAccountCount
FROM 
  v_finance_mapped;