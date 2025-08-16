%sql
-- Extract, transform and load finance data from ECC Everest into Finance table
INSERT OVERWRITE TABLE Finance.finance
WITH 
-- Step 1: Get data from source tables
source_data AS (
    SELECT 
        f.rbukrs AS CompCode,
        f.gjahr AS FiscalYear,
        f.poper AS PostingPeriod,
        f.belnr AS DocumentNumber,
        f.racct AS GLAccount,
        f.rcntr AS CostCenter,
        f.prctr AS ProfitCenter,
        f.rbusa AS TradingPartner,
        f.rhcur AS LocalCurrency,
        f.rtcur AS TransactionCurrency,
        f.hsl AS LocalCurrencyAmount,
        f.tsl AS TransactionCurrencyAmount,
        b.augbl AS OffsetClearingDocumentNumber,
        b.augdt AS ClearingDate,
        b.saknr AS OffsetAccount
    FROM 
        ECC_Everest.FAGLFLEXA f
    LEFT JOIN 
        ECC_Everest.BSEG b
    ON 
        f.rbukrs = b.bukrs AND
        f.gjahr = b.gjahr AND
        f.belnr = b.belnr
),

-- Step 2: Join with Golden Entity view to get Legal Entity
entity_data AS (
    SELECT 
        s.*,
        e.LegalEntity
    FROM 
        source_data s
    LEFT JOIN 
        Golden.v_entity e
    ON 
        s.CompCode = e.CompCode
    WHERE 
        e.IsArchived = 0 -- Filter out archived Golden Entity values
),

-- Step 3: Join with Golden GL view to get Golden GL Account
gl_data AS (
    SELECT 
        e.*,
        g.GoldenGLAcct
    FROM 
        entity_data e
    LEFT JOIN 
        Golden.v_gl g
    ON 
        e.GLAccount = g.GLAccount
),

-- Step 4: Join with Golden Trading Partner view
tp_data AS (
    SELECT 
        g.*,
        tp.GoldenTradingPartner
    FROM 
        gl_data g
    LEFT JOIN 
        Golden.v_trading_partner tp
    ON 
        g.TradingPartner = tp.TradingPartner
),

-- Step 5: Join with Golden GL view again to get Golden Offset Account
offset_data AS (
    SELECT 
        t.*,
        g.GoldenGLAcct AS GoldenOffsetAccount
    FROM 
        tp_data t
    LEFT JOIN 
        Golden.v_gl g
    ON 
        t.OffsetAccount = g.GLAccount
),

-- Step 6: Get exchange rates from BPC
exchange_rates AS (
    SELECT 
        FromCurrency,
        ToCurrency,
        FiscalYear,
        Period,
        Rate
    FROM 
        s_shared.v_actual_exchange_rate_bpc
),

-- Step 7: Calculate Gain/Loss amounts
final_data AS (
    SELECT 
        o.FiscalYear,
        o.PostingPeriod,
        o.DocumentNumber,
        o.CompCode,
        o.LegalEntity,
        o.GLAccount,
        o.GoldenGLAcct,
        o.TradingPartner,
        o.GoldenTradingPartner,
        -- Calculate GainLossGC (Group Currency)
        CASE 
            WHEN er_gc.Rate IS NOT NULL THEN (o.LocalCurrencyAmount * er_gc.Rate) - o.LocalCurrencyAmount
            ELSE 0
        END AS GainLossGC,
        -- Calculate GainLossLC (Local Currency)
        CASE 
            WHEN o.LocalCurrencyAmount <> 0 AND o.TransactionCurrencyAmount <> 0 
            THEN o.LocalCurrencyAmount - o.TransactionCurrencyAmount
            ELSE 0
        END AS GainLossLC,
        o.LocalCurrency,
        -- Calculate GainLossTC (Transaction Currency)
        CASE 
            WHEN o.TransactionCurrencyAmount <> 0 AND o.LocalCurrencyAmount <> 0 
            THEN o.TransactionCurrencyAmount - o.LocalCurrencyAmount
            ELSE 0
        END AS GainLossTC,
        o.TransactionCurrency,
        o.OffsetAccount,
        o.GoldenOffsetAccount,
        o.LocalCurrencyAmount AS OffsetAccountLCAmount,
        o.TransactionCurrencyAmount AS OffsetAccountTCAmount,
        o.OffsetClearingDocumentNumber,
        'ECC Everest' AS SourceSystem,
        current_timestamp() AS LoadTimestamp
    FROM 
        offset_data o
    LEFT JOIN 
        exchange_rates er_gc
    ON 
        o.LocalCurrency = er_gc.FromCurrency
        AND 'USD' = er_gc.ToCurrency -- Assuming USD is the group currency
        AND o.FiscalYear = er_gc.FiscalYear
        AND o.PostingPeriod = er_gc.Period
)

-- Final select to insert into target table
SELECT 
    FiscalYear,
    PostingPeriod,
    DocumentNumber,
    CompCode,
    LegalEntity,
    GLAccount,
    GoldenGLAcct,
    TradingPartner,
    GoldenTradingPartner,
    GainLossGC,
    GainLossLC,
    LocalCurrency,
    GainLossTC,
    TransactionCurrency,
    OffsetAccount,
    GoldenOffsetAccount,
    OffsetAccountLCAmount,
    OffsetAccountTCAmount,
    OffsetClearingDocumentNumber,
    SourceSystem,
    LoadTimestamp
FROM 
    final_data;