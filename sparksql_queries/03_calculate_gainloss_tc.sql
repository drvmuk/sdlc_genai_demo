%sql
-- Calculate GainLossTC (Transaction Currency)
-- This query calculates the gain/loss in transaction currency

CREATE OR REPLACE TEMPORARY VIEW v_finance_gainloss AS
SELECT 
  t.*,
  CASE 
    WHEN t.RTCUR != t.RHCUR AND t.OSL != 0 THEN t.GainLossLC / t.ExchangeRate
    ELSE 0 
  END AS GainLossTC,
  CASE
    WHEN t.KOART = 'S' THEN t.HKONT
    WHEN t.KOART = 'D' THEN t.HKONT
    WHEN t.KOART = 'K' THEN t.HKONT
    ELSE NULL
  END AS OffsetAccount
FROM 
  v_finance_transformed t;