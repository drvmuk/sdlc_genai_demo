%sql
-- Validation Query 1: Verify source tables exist and are accessible
SELECT 
  table_catalog, 
  table_schema, 
  table_name, 
  COUNT(*) AS record_count
FROM (
  SELECT table_catalog, table_schema, table_name FROM system.information_schema.tables 
  WHERE (table_schema = 'b_lots' AND table_name IN ('plooh', 'plool', 'plcbe', 'plcam', 'phihm', 'lecotw1', 'lmpmt03'))
     OR (table_schema = 'lots_staging' AND table_name = 'PLPMS_stg')
     OR (table_schema = 's_shared' AND table_name IN ('currency_conversion', 'isc_registry'))
     OR (table_schema = 'b_user_managed' AND table_name IN ('rtbl_lots_ref_type_category_bo_scope', 'rtbl_lots_market_region_cco'))
) t
GROUP BY table_catalog, table_schema, table_name
ORDER BY table_schema, table_name;

%sql
-- Validation Query 2: Check for data completeness in key source tables
SELECT 
  'b_lots.plooh' AS table_name,
  COUNT(*) AS total_records,
  SUM(CASE WHEN refno IS NULL THEN 1 ELSE 0 END) AS null_refno_count,
  SUM(CASE WHEN refto IS NULL THEN 1 ELSE 0 END) AS null_refto_count,
  SUM(CASE WHEN DATA_OPERATION = 'D' THEN 1 ELSE 0 END) AS deleted_records
FROM b_lots.plooh
UNION ALL
SELECT 
  'b_lots.plool' AS table_name,
  COUNT(*) AS total_records,
  SUM(CASE WHEN refno IS NULL THEN 1 ELSE 0 END) AS null_refno_count,
  SUM(CASE WHEN refto IS NULL THEN 1 ELSE 0 END) AS null_refto_count,
  SUM(CASE WHEN DATA_OPERATION = 'D' THEN 1 ELSE 0 END) AS deleted_records
FROM b_lots.plool;

%sql
-- Validation Query 3: Check for data quality in reference tables
SELECT 
  'b_user_managed.rtbl_lots_ref_type_category_bo_scope' AS table_name,
  COUNT(*) AS total_records,
  COUNT(DISTINCT bo_scope) AS distinct_bo_scope_count
FROM b_user_managed.rtbl_lots_ref_type_category_bo_scope
UNION ALL
SELECT 
  'b_user_managed.rtbl_lots_market_region_cco' AS table_name,
  COUNT(*) AS total_records,
  COUNT(DISTINCT market_id) AS distinct_market_id_count
FROM b_user_managed.rtbl_lots_market_region_cco;

%sql
-- Validation Query 4: Check for target table existence after creation
SELECT 
  table_catalog, 
  table_schema, 
  table_name, 
  COUNT(*) AS record_count
FROM system.information_schema.tables 
WHERE (table_schema = 's_shared' AND table_name IN ('sales_orders_hdr_itm_lots', 'sales_orders'))
GROUP BY table_catalog, table_schema, table_name;

%sql
-- Validation Query 5: Data quality check on final sales_orders table
SELECT
  COUNT(*) AS total_records,
  COUNT(DISTINCT OrderNumber) AS distinct_orders,
  COUNT(DISTINCT OrderLineNumber) AS distinct_order_lines,
  MIN(OrderDate) AS min_order_date,
  MAX(OrderDate) AS max_order_date,
  SUM(CASE WHEN OrderStatus IS NULL THEN 1 ELSE 0 END) AS null_order_status_count,
  SUM(CASE WHEN OrderQuantity < 0 THEN 1 ELSE 0 END) AS negative_quantity_count
FROM s_shared.sales_orders
WHERE OrderDate >= date_add(current_date(), -1095); -- Last 3 years