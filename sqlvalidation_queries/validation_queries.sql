%sql
-- Validation query for sales_orders_comp_stg
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CompOrderNumber) AS distinct_order_numbers,
  COUNT(DISTINCT CompLineNumber) AS distinct_line_numbers,
  SUM(CompOrderQuantityOriginal) AS total_order_quantity,
  SUM(CompOpenQuantityOrginal) AS total_open_quantity,
  MIN(CompCreateDate) AS earliest_create_date,
  MAX(CompCreateDate) AS latest_create_date
FROM sales_orders_comp_stg;

-- Validation query for sales_orders_comp
%sql
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CompOrderNumber) AS distinct_order_numbers,
  COUNT(DISTINCT CompLineNumber) AS distinct_line_numbers,
  SUM(CompOrderQuantityOriginal) AS total_order_quantity,
  SUM(CompOpenQuantityOrginal) AS total_open_quantity,
  MIN(CompCreateDate) AS earliest_create_date,
  MAX(CompCreateDate) AS latest_create_date,
  COUNT(CASE WHEN CompSiteId IS NOT NULL THEN 1 END) AS site_id_count,
  COUNT(CASE WHEN CompShipToName IS NOT NULL THEN 1 END) AS ship_to_name_count,
  COUNT(CASE WHEN CompSoldToName IS NOT NULL THEN 1 END) AS sold_to_name_count,
  COUNT(CASE WHEN CompSalesDistributionChannel IS NOT NULL THEN 1 END) AS dist_channel_count
FROM sales_orders_comp;

-- Check for data consistency between the two tables
%sql
SELECT 
  'Staging' AS table_name,
  COUNT(*) AS row_count,
  COUNT(DISTINCT CompOrderNumber) AS distinct_orders
FROM sales_orders_comp_stg
UNION ALL
SELECT 
  'Final' AS table_name,
  COUNT(*) AS row_count,
  COUNT(DISTINCT CompOrderNumber) AS distinct_orders
FROM sales_orders_comp;

-- Validate specific transformations
%sql
SELECT 
  a.CompOrderNumber,
  a.CompLineNumber,
  a.CompSiteId,
  a.CompShipToName,
  a.CompSoldToName,
  a.CompExchangeRate,
  a.CompSalesDistributionChannel,
  a.CompActualShipDate,
  a.CompLatestProcessingDate
FROM sales_orders_comp a
LIMIT 100;