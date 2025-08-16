%sql
-- Validate source data availability and structure
-- This query checks if the source tables exist and have data

SELECT 
  'FAGLFLEXA' AS TableName, 
  COUNT(*) AS RecordCount 
FROM 
  ECC_Everest.FAGLFLEXA
UNION ALL
SELECT 
  'BSEG' AS TableName, 
  COUNT(*) AS RecordCount 
FROM 
  ECC_Everest.BSEG
UNION ALL
SELECT 
  'Entity Golden View' AS TableName, 
  COUNT(*) AS RecordCount 
FROM 
  s_shared.v_revenue_entity_bpc
UNION ALL
SELECT 
  'BPC Exchange Rates' AS TableName, 
  COUNT(*) AS RecordCount 
FROM 
  s_shared.v_actual_exchange_rate_bpc;