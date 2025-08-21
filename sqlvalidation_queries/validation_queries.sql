%sql
-- Validate the intermediate table creation
SELECT COUNT(*) AS record_count 
FROM s_shared.sales_orders_hdr_itm_lots;

-- Check for any null values in key fields
%sql
SELECT COUNT(*) AS null_order_numbers
FROM s_shared.sales_orders_hdr_itm_lots
WHERE OrderNumber IS NULL;

-- Validate the final sales_orders table creation
%sql
SELECT COUNT(*) AS record_count 
FROM s_shared.sales_orders;

-- Check data distribution by year
%sql
SELECT 
  YEAR(CreateDate) AS order_year,
  COUNT(*) AS order_count
FROM s_shared.sales_orders
GROUP BY YEAR(CreateDate)
ORDER BY order_year;

-- Validate the join with currency conversion
%sql
SELECT 
  CurrencyType,
  COUNT(*) AS record_count,
  COUNT(DISTINCT ExchangeRate) AS distinct_exchange_rates
FROM s_shared.sales_orders
GROUP BY CurrencyType
ORDER BY record_count DESC;

-- Check SiteId population from multiple sources
%sql
SELECT 
  CASE 
    WHEN SiteId IS NULL THEN 'NULL'
    WHEN SiteId = '' THEN 'EMPTY'
    ELSE 'POPULATED'
  END AS site_id_status,
  COUNT(*) AS record_count
FROM s_shared.sales_orders
GROUP BY 
  CASE 
    WHEN SiteId IS NULL THEN 'NULL'
    WHEN SiteId = '' THEN 'EMPTY'
    ELSE 'POPULATED'
  END;

-- Check SalesDistributionChannel population
%sql
SELECT 
  CASE 
    WHEN SalesDistributionChannel IS NULL THEN 'NULL'
    WHEN SalesDistributionChannel = '' THEN 'EMPTY'
    ELSE 'POPULATED'
  END AS channel_status,
  COUNT(*) AS record_count
FROM s_shared.sales_orders
GROUP BY 
  CASE 
    WHEN SalesDistributionChannel IS NULL THEN 'NULL'
    WHEN SalesDistributionChannel = '' THEN 'EMPTY'
    ELSE 'POPULATED'
  END;

-- Validate quantity calculations
%sql
SELECT 
  SUM(OrderQuantityOriginal) AS total_order_qty,
  SUM(CancelledQuantityOriginal) AS total_cancelled_qty,
  SUM(DeliveredQuantityOriginal) AS total_delivered_qty,
  SUM(OpenQuantityOrginal) AS total_open_qty
FROM s_shared.sales_orders;

-- Validate that open quantity equals order minus cancelled minus delivered
%sql
SELECT 
  COUNT(*) AS mismatched_records
FROM s_shared.sales_orders
WHERE ABS(OpenQuantityOrginal - (OrderQuantityOriginal - CancelledQuantityOriginal - DeliveredQuantityOriginal)) > 0.001;