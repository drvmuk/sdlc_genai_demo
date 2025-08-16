%sql
-- Apply transformations and retrieve exchange rates
-- This query enriches the joined data with entity information
-- and prepares for gain/loss calculations

CREATE OR REPLACE TEMPORARY VIEW v_finance_transformed AS
SELECT 
  j.*,
  e.EntityID,
  e.EntityName,
  e.EntityType,
  e.EntityRegion,
  r.FromCurrency,
  r.ToCurrency,
  r.ExchangeRate,
  r.EffectiveDate,
  CASE 
    WHEN j.RTCUR != j.RHCUR AND j.OSL != 0 THEN j.HSL - (j.OSL * r.ExchangeRate)
    ELSE 0 
  END AS GainLossLC,
  CASE 
    WHEN j.RTCUR != j.RKCUR AND j.OSL != 0 THEN j.KSL - (j.OSL * r.ExchangeRate)
    ELSE 0 
  END AS GainLossGC
FROM 
  v_finance_joined j
LEFT JOIN 
  s_shared.v_revenue_entity_bpc e
ON 
  j.CompanyCode = e.CompanyCode
LEFT JOIN 
  s_shared.v_actual_exchange_rate_bpc r
ON 
  j.RTCUR = r.FromCurrency
  AND (
    (j.RHCUR = r.ToCurrency) OR 
    (j.RKCUR = r.ToCurrency)
  )
  AND r.EffectiveDate = (
    SELECT MAX(EffectiveDate) 
    FROM s_shared.v_actual_exchange_rate_bpc 
    WHERE EffectiveDate <= CONCAT(j.FiscalYear, LPAD(j.Period, 2, '0'), '01')
  );