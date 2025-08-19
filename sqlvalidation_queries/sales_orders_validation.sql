-- Validation query for s_shared.sales_orders_hdr_itm_lots
%sql
SELECT COUNT(*) AS total_records_intermediate_table
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation query to check for NULL values in key fields of intermediate table
%sql
SELECT 
    SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
    SUM(CASE WHEN MaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers,
    SUM(CASE WHEN LineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_numbers
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation query for final sales_orders table
%sql
SELECT COUNT(*) AS total_records_final_table
FROM s_shared.sales_orders;

-- Validation query to check distribution of orders by year
%sql
SELECT 
    YEAR(CreateDate) AS order_year,
    COUNT(*) AS order_count
FROM s_shared.sales_orders
GROUP BY YEAR(CreateDate)
ORDER BY order_year;

-- Validation query to check for any missing joins
%sql
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN SourceSystem IS NULL THEN 1 ELSE 0 END) AS missing_source_system,
    SUM(CASE WHEN SiteId IS NULL THEN 1 ELSE 0 END) AS missing_site_id,
    SUM(CASE WHEN ShipToName IS NULL THEN 1 ELSE 0 END) AS missing_ship_to_name,
    SUM(CASE WHEN SoldToName IS NULL THEN 1 ELSE 0 END) AS missing_sold_to_name,
    SUM(CASE WHEN PayerName IS NULL THEN 1 ELSE 0 END) AS missing_payer_name
FROM s_shared.sales_orders;

-- Validation query to check currency exchange rate application
%sql
SELECT 
    CurrencyType,
    COUNT(*) AS record_count,
    AVG(ExchangeRate) AS avg_exchange_rate
FROM s_shared.sales_orders
GROUP BY CurrencyType
ORDER BY record_count DESC;

-- Validation query to check order status distribution
%sql
SELECT 
    OrderStatusHeader,
    OrderStatusLineLast,
    COUNT(*) AS record_count
FROM s_shared.sales_orders
GROUP BY OrderStatusHeader, OrderStatusLineLast
ORDER BY record_count DESC;