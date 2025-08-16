%sql
-- Validate Finance table data
-- This query checks the data loaded into the Finance table

SELECT 
  COUNT(*) AS TotalRecords,
  MIN(ProcessedTimestamp) AS OldestRecord,
  MAX(ProcessedTimestamp) AS NewestRecord,
  COUNT(DISTINCT CompanyCode) AS CompanyCodeCount,
  COUNT(DISTINCT FiscalYear) AS FiscalYearCount,
  COUNT(DISTINCT Period) AS PeriodCount,
  SUM(GainLossLC) AS TotalGainLossLC,
  SUM(GainLossGC) AS TotalGainLossGC,
  SUM(GainLossTC) AS TotalGainLossTC
FROM 
  Finance.Finance_Data;