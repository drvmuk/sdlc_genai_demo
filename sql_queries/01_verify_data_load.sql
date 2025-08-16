%sql
-- Verify data load by checking record counts
SELECT 
    FiscalYear,
    PostingPeriod,
    COUNT(*) AS RecordCount,
    SUM(GainLossGC) AS TotalGainLossGC,
    SUM(GainLossLC) AS TotalGainLossLC,
    SUM(GainLossTC) AS TotalGainLossTC
FROM 
    Finance.finance
WHERE 
    LoadTimestamp > date_sub(current_timestamp(), 1)
GROUP BY 
    FiscalYear, 
    PostingPeriod
ORDER BY 
    FiscalYear, 
    PostingPeriod;