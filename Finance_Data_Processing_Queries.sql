-- This file contains the key SQL queries used in the Finance Data Processing Job

-- Step 1: Join FAGLFLEXA with BSEG on document number, fiscal year, and company code for records with XBILK = 'X'
SELECT 
    F.RLDNR AS Ledger,
    F.RBUKRS AS CompCode,
    F.GJAHR AS FiscalYear,
    F.POPER AS Period,
    CONCAT(F.GJAHR, LPAD(F.POPER, 2, '0')) AS FiscalYearPeriod,
    F.DOCNR AS DocumentNumber,
    F.RRCTY AS RecordType,
    F.RACCT AS GLAccount,
    F.RCNTR AS CostCenter,
    F.PRCTR AS ProfitCenter,
    F.RFAREA AS FunctionalArea,
    F.RBUSA AS BusinessArea,
    F.KOKRS AS ControllingArea,
    F.PRCTR AS Segment,
    F.HSLVT AS AmountLC,
    F.HSL AS AmountGC,
    F.RHCUR AS LocalCurrency,
    F.RKCUR AS GroupCurrency,
    F.RUNIT AS BaseUnit,
    F.RTCUR AS TransactionCurrency,
    F.TSL AS AmountTC,
    B.XBILK,
    B.BSCHL AS PostingKey,
    B.ZUONR AS Assignment,
    B.SGTXT AS ItemText
FROM 
    Everest_ECC.FAGLFLEXA F
JOIN 
    Everest_ECC.BSEG B
ON 
    F.DOCNR = B.BELNR 
    AND F.GJAHR = B.GJAHR 
    AND F.RBUKRS = B.BUKRS
WHERE 
    B.XBILK = 'X';

-- Step 2: Filter out records with CompCode starting with '8%'
SELECT *
FROM joined_data
WHERE CompCode NOT LIKE '8%';

-- Step 3: Derive Legal Entity from Golden Entity view
SELECT 
    F.*,
    E.EntityID,
    E.EntityName,
    E.LegalEntityName
FROM 
    filtered_data F
LEFT JOIN 
    Golden.Entity E
ON 
    F.CompCode = E.CompanyCode
WHERE 
    E.IsArchived = FALSE OR E.IsArchived IS NULL;

-- Step 4: Derive GLAccount information from GL golden view
SELECT 
    D.*,
    G.GoldenGLAccount,
    G.GLAccountName,
    G.GLAccountType,
    G.IsRealizedAccount,
    G.IsUnrealizedAccount
FROM 
    data_with_entity D
LEFT JOIN 
    Golden.GL G
ON 
    D.GLAccount = G.SourceGLAccount
WHERE
    (G.IsRealizedAccount = TRUE OR G.IsUnrealizedAccount = TRUE);

-- Step 5: Calculate GainLossGC using BPC exchange rates
SELECT 
    D.*,
    CASE 
        WHEN D.LocalCurrency = D.GroupCurrency THEN 0
        ELSE D.AmountLC * ER.ExchangeRate - D.AmountGC
    END AS GainLossGC
FROM 
    data_with_gl D
LEFT JOIN 
    BPC.s_shared.v_actual_exchange_rate_bpc ER
ON 
    D.FiscalYearPeriod = ER.FiscalYearPeriod
    AND D.LocalCurrency = ER.FromCurrency
    AND D.GroupCurrency = ER.ToCurrency;

-- Step 6: Derive GainLossLC based on LocalCurrency
SELECT 
    D.*,
    CASE 
        WHEN D.LocalCurrency = D.TransactionCurrency THEN 0
        ELSE D.AmountTC * ER.ExchangeRate - D.AmountLC
    END AS GainLossLC
FROM 
    data_with_gainloss_gc D
LEFT JOIN 
    BPC.s_shared.v_actual_exchange_rate_bpc ER
ON 
    D.FiscalYearPeriod = ER.FiscalYearPeriod
    AND D.TransactionCurrency = ER.FromCurrency
    AND D.LocalCurrency = ER.ToCurrency;

-- Step 7: Derive additional fields and prepare final dataset
SELECT 
    D.*,
    CASE 
        WHEN D.TransactionCurrency = D.GroupCurrency THEN 0
        ELSE D.AmountTC * ER.ExchangeRate - D.AmountGC
    END AS GainLossTC,
    CASE 
        WHEN D.PostingKey IN ('40', '50') THEN 
            (SELECT MIN(RACCT) FROM Everest_ECC.FAGLFLEXA WHERE DOCNR = D.DocumentNumber AND RACCT != D.GLAccount)
        ELSE NULL
    END AS OffsetAccount,
    CASE 
        WHEN D.PostingKey IN ('40', '50') THEN 
            (SELECT MIN(G.GoldenGLAccount) 
             FROM Golden.GL G 
             JOIN Everest_ECC.FAGLFLEXA F ON G.SourceGLAccount = F.RACCT 
             WHERE F.DOCNR = D.DocumentNumber AND F.RACCT != D.GLAccount)
        ELSE NULL
    END AS GoldenOffsetAccount,
    CURRENT_TIMESTAMP() AS LoadTimestamp
FROM 
    data_with_gainloss_lc D
LEFT JOIN 
    BPC.s_shared.v_actual_exchange_rate_bpc ER
ON 
    D.FiscalYearPeriod = ER.FiscalYearPeriod
    AND D.TransactionCurrency = ER.FromCurrency
    AND D.GroupCurrency = ER.ToCurrency;

-- Summary statistics query
SELECT 
    COUNT(*) AS TotalRecords,
    COUNT(DISTINCT CompCode) AS UniqueCompanyCodes,
    COUNT(DISTINCT EntityID) AS UniqueEntities,
    COUNT(DISTINCT FiscalYearPeriod) AS UniqueFiscalPeriods,
    SUM(CASE WHEN GainLossGC != 0 THEN 1 ELSE 0 END) AS RecordsWithGainLossGC,
    SUM(CASE WHEN GainLossLC != 0 THEN 1 ELSE 0 END) AS RecordsWithGainLossLC,
    SUM(CASE WHEN GainLossTC != 0 THEN 1 ELSE 0 END) AS RecordsWithGainLossTC
FROM Finance.FinanceGainLoss;