%sql
-- Validate source tables exist and contain data
SELECT 'plooh' as table_name, COUNT(*) as record_count FROM b_lots.plooh
UNION ALL
SELECT 'plool' as table_name, COUNT(*) as record_count FROM b_lots.plool
UNION ALL
SELECT 'plcbe' as table_name, COUNT(*) as record_count FROM b_lots.plcbe
UNION ALL
SELECT 'plcam' as table_name, COUNT(*) as record_count FROM b_lots.plcam
UNION ALL
SELECT 'plpms' as table_name, COUNT(*) as record_count FROM b_lots.plpms
UNION ALL
SELECT 'phihm' as table_name, COUNT(*) as record_count FROM b_lots.phihm
UNION ALL
SELECT 'lecotw1' as table_name, COUNT(*) as record_count FROM b_lots.lecotw1
UNION ALL
SELECT 'lmpmt03' as table_name, COUNT(*) as record_count FROM b_lots.lmpmt03
UNION ALL
SELECT 'isc_registry' as table_name, COUNT(*) as record_count FROM s_shared.isc_registry
UNION ALL
SELECT 'currency_conversion' as table_name, COUNT(*) as record_count FROM s_shared.currency_conversion
UNION ALL
SELECT 'rtbl_lots_ref_type_category_bo_scope' as table_name, COUNT(*) as record_count FROM b_user_managed.rtbl_lots_ref_type_category_bo_scope
UNION ALL
SELECT 'rtbl_lots_market_region_cco' as table_name, COUNT(*) as record_count FROM b_user_managed.rtbl_lots_market_region_cco;

%sql
-- Validate intermediate table creation and data population
SELECT COUNT(*) as record_count FROM s_shared.sales_orders_hdr_itm_lots;

%sql
-- Validate key columns in intermediate table have no nulls
SELECT 
  SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) as OrderNumber_nulls,
  SUM(CASE WHEN OrderDate IS NULL THEN 1 ELSE 0 END) as OrderDate_nulls,
  SUM(CASE WHEN CustomerNumber IS NULL THEN 1 ELSE 0 END) as CustomerNumber_nulls,
  SUM(CASE WHEN ItemNumber IS NULL THEN 1 ELSE 0 END) as ItemNumber_nulls
FROM s_shared.sales_orders_hdr_itm_lots;

%sql
-- Validate main table creation and data population
SELECT COUNT(*) as record_count FROM sales_orders;

%sql
-- Validate key columns in main table have no nulls
SELECT 
  SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) as OrderNumber_nulls,
  SUM(CASE WHEN OrderDate IS NULL THEN 1 ELSE 0 END) as OrderDate_nulls,
  SUM(CASE WHEN CustomerNumber IS NULL THEN 1 ELSE 0 END) as CustomerNumber_nulls,
  SUM(CASE WHEN ItemNumber IS NULL THEN 1 ELSE 0 END) as ItemNumber_nulls
FROM sales_orders;

%sql
-- Validate data integrity - check for duplicate orders
SELECT OrderNumber, COUNT(*) as duplicate_count
FROM sales_orders
GROUP BY OrderNumber
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 100;

%sql
-- Validate currency conversion is applied correctly
SELECT 
  COUNT(*) as total_orders,
  SUM(CASE WHEN OrderAmountUSD IS NULL AND OrderAmount IS NOT NULL THEN 1 ELSE 0 END) as missing_usd_conversion
FROM sales_orders;

%sql
-- Validate referential integrity with reference tables
SELECT 
  COUNT(*) as total_orders,
  SUM(CASE WHEN market_region IS NOT NULL AND NOT EXISTS 
      (SELECT 1 FROM b_user_managed.rtbl_lots_market_region_cco r WHERE sales_orders.market_region = r.market_region)
      THEN 1 ELSE 0 END) as invalid_market_regions
FROM sales_orders;