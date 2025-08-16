%sql
-- Validate exchange rates used in calculations
WITH missing_rates AS (
    SELECT DISTINCT
        f.FiscalYear,
        f.PostingPeriod,
        f.LocalCurrency,
        'USD' AS GroupCurrency -- Assuming USD is the group currency
    FROM 
        Finance.finance f
    LEFT JOIN 
        s_shared.v_actual_exchange_rate_bpc er
    ON 
        f.LocalCurrency = er.FromCurrency
        AND 'USD' = er.ToCurrency
        AND f.FiscalYear = er.FiscalYear
        AND f.PostingPeriod = er.Period
    WHERE 
        er.Rate IS NULL
        AND f.LoadTimestamp > date_sub(current_timestamp(), 1)
)

SELECT 
    FiscalYear,
    PostingPeriod,
    LocalCurrency,
    GroupCurrency,
    'Missing Exchange Rate' AS Issue
FROM 
    missing_rates
ORDER BY 
    FiscalYear, 
    PostingPeriod, 
    LocalCurrency;