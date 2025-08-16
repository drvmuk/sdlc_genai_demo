%sql
-- Data quality check to identify potential issues
SELECT 
    'Missing Legal Entity' AS Issue,
    COUNT(*) AS RecordCount
FROM 
    Finance.finance
WHERE 
    LegalEntity IS NULL
    AND LoadTimestamp > date_sub(current_timestamp(), 1)

UNION ALL

SELECT 
    'Missing Golden GL Account' AS Issue,
    COUNT(*) AS RecordCount
FROM 
    Finance.finance
WHERE 
    GoldenGLAcct IS NULL
    AND LoadTimestamp > date_sub(current_timestamp(), 1)

UNION ALL

SELECT 
    'Missing Golden Trading Partner' AS Issue,
    COUNT(*) AS RecordCount
FROM 
    Finance.finance
WHERE 
    TradingPartner IS NOT NULL
    AND GoldenTradingPartner IS NULL
    AND LoadTimestamp > date_sub(current_timestamp(), 1)

UNION ALL

SELECT 
    'Missing Offset Account' AS Issue,
    COUNT(*) AS RecordCount
FROM 
    Finance.finance
WHERE 
    OffsetAccount IS NULL
    AND LoadTimestamp > date_sub(current_timestamp(), 1);