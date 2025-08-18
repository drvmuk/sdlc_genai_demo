%sql
-- Validate source data availability
SELECT 'plooh' as table_name, count(*) as record_count FROM b_lots.plooh
UNION ALL
SELECT 'plool' as table_name, count(*) as record_count FROM b_lots.plool
UNION ALL
SELECT 'plcam' as table_name, count(*) as record_count FROM b_lots.plcam
UNION ALL
SELECT 'plcbe' as table_name, count(*) as record_count FROM b_lots.plcbe
UNION ALL
SELECT 'ploal' as table_name, count(*) as record_count FROM b_lots.ploal
UNION ALL
SELECT 'plosl' as table_name, count(*) as record_count FROM b_lots.plosl;

-- Validate intermediate table creation
%sql
SELECT count(*) as record_count FROM s_shared.sales_orders_hdr_itm_lots;

-- Validate data quality in intermediate table
%sql
SELECT 
  COUNT(*) as total_records,
  SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) as null_order_numbers,
  SUM(CASE WHEN OrderType IS NULL THEN 1 ELSE 0 END) as null_order_types,
  SUM(CASE WHEN DATA_OPERATION = 'D' THEN 1 ELSE 0 END) as deleted_records
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validate final table creation
%sql
SELECT count(*) as record_count FROM sales_orders;

-- Validate data quality in final table
%sql
SELECT 
  COUNT(*) as total_records,
  SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) as null_order_numbers,
  SUM(CASE WHEN OrderCreationDate IS NULL THEN 1 ELSE 0 END) as null_creation_dates,
  MIN(OrderCreationDate) as min_creation_date,
  MAX(OrderCreationDate) as max_creation_date
FROM sales_orders;

-- Validate join quality
%sql
SELECT 
  COUNT(*) as total_records,
  SUM(CASE WHEN SalesDistributionChannel IS NULL THEN 1 ELSE 0 END) as missing_distribution_channel,
  SUM(CASE WHEN SoldToName IS NULL THEN 1 ELSE 0 END) as missing_sold_to_name,
  SUM(CASE WHEN ShipToName IS NULL THEN 1 ELSE 0 END) as missing_ship_to_name,
  SUM(CASE WHEN SiteId IS NULL THEN 1 ELSE 0 END) as missing_site_id
FROM sales_orders;

-- Validate date filter is working correctly (last 3 years of data)
%sql
SELECT 
  YEAR(OrderCreationDate) as order_year,
  COUNT(*) as record_count
FROM sales_orders
GROUP BY YEAR(OrderCreationDate)
ORDER BY order_year DESC;