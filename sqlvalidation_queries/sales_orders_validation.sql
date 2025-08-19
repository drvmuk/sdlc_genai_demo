%sql
-- Validation query 1: Check if intermediate table was created successfully
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT OrderNumber) AS distinct_orders,
    MIN(CreateDate) AS earliest_date,
    MAX(CreateDate) AS latest_date
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation query 2: Check if main table was created successfully
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT OrderNumber) AS distinct_orders,
    MIN(CreateDate) AS earliest_date,
    MAX(CreateDate) AS latest_date
FROM s_shared.sales_orders;

-- Validation query 3: Check for NULL values in critical columns
SELECT 
    SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
    SUM(CASE WHEN MaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers,
    SUM(CASE WHEN LineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_numbers
FROM s_shared.sales_orders;

-- Validation query 4: Check if date filtering is working correctly
SELECT 
    YEAR(CreateDate) AS order_year,
    COUNT(*) AS record_count
FROM s_shared.sales_orders
GROUP BY YEAR(CreateDate)
ORDER BY order_year;

-- Validation query 5: Verify SiteId logic is working correctly
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN SiteId IS NOT NULL THEN 1 ELSE 0 END) AS records_with_siteid,
    (SUM(CASE WHEN SiteId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_with_siteid
FROM s_shared.sales_orders;

-- Validation query 6: Check exchange rate calculation
SELECT 
    CurrencyType,
    COUNT(*) AS record_count,
    AVG(ExchangeRate) AS avg_exchange_rate,
    MIN(ExchangeRate) AS min_exchange_rate,
    MAX(ExchangeRate) AS max_exchange_rate
FROM s_shared.sales_orders
GROUP BY CurrencyType
ORDER BY record_count DESC;

-- Validation query 7: Verify the join with customer data is working
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN ShipToName IS NOT NULL THEN 1 ELSE 0 END) AS records_with_ship_to_name,
    SUM(CASE WHEN SoldToName IS NOT NULL THEN 1 ELSE 0 END) AS records_with_sold_to_name,
    SUM(CASE WHEN PayerName IS NOT NULL THEN 1 ELSE 0 END) AS records_with_payer_name
FROM s_shared.sales_orders;

-- Validation query 8: Check quantity calculations
SELECT 
    AVG(OrderQuantityOriginal) AS avg_order_qty,
    AVG(OpenQuantityOrginal) AS avg_open_qty,
    AVG(DeliveredQuantityOriginal) AS avg_delivered_qty,
    AVG(CancelledQuantityOriginal) AS avg_cancelled_qty
FROM s_shared.sales_orders;

-- Validation query 9: Check distribution of order status
SELECT 
    OrderStatusHeader,
    OrderStatusLineLast,
    COUNT(*) AS record_count
FROM s_shared.sales_orders
GROUP BY OrderStatusHeader, OrderStatusLineLast
ORDER BY record_count DESC
LIMIT 20;

-- Validation query 10: Check for data consistency between source and target tables
SELECT 
    COUNT(*) AS matching_records
FROM s_shared.sales_orders a
JOIN s_shared.sales_orders_hdr_itm_lots b
ON a.OrderNumber = b.OrderNumber AND a.LineNumber = b.LineNumber
WHERE a.OrderQuantityOriginal = b.OrderQuantityOriginal
AND a.CreateDate = b.CreateDate;