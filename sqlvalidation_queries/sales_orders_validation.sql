%sql
-- Validation query 1: Check if intermediate table was created correctly
SELECT COUNT(*) AS record_count 
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation query 2: Check for null values in key columns of intermediate table
SELECT 
    SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN OrderType IS NULL THEN 1 ELSE 0 END) AS null_order_type,
    SUM(CASE WHEN LineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN MaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation query 3: Check if final table was created correctly
SELECT COUNT(*) AS record_count 
FROM s_shared.sales_orders;

-- Validation query 4: Check for null values in key columns of final table
SELECT 
    SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN OrderType IS NULL THEN 1 ELSE 0 END) AS null_order_type,
    SUM(CASE WHEN LineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN MaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number
FROM s_shared.sales_orders;

-- Validation query 5: Check date filtering is working correctly
SELECT 
    YEAR(CreateDate) AS order_year,
    COUNT(*) AS record_count
FROM s_shared.sales_orders
GROUP BY YEAR(CreateDate)
ORDER BY order_year;

-- Validation query 6: Verify SiteId logic is working correctly
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

-- Validation query 7: Check currency exchange rate logic
SELECT 
    CurrencyType,
    COUNT(*) AS record_count,
    AVG(ExchangeRate) AS avg_exchange_rate
FROM s_shared.sales_orders
GROUP BY CurrencyType
ORDER BY record_count DESC;

-- Validation query 8: Verify quantity calculations
SELECT 
    SUM(OrderQuantityOriginal) AS total_order_qty,
    SUM(CancelledQuantityOriginal) AS total_cancelled_qty,
    SUM(DeliveredQuantityOriginal) AS total_delivered_qty,
    SUM(OpenQuantityOrginal) AS total_open_qty,
    SUM(OrderQuantityOriginal - CancelledQuantityOriginal - DeliveredQuantityOriginal) AS calculated_open_qty,
    ABS(SUM(OpenQuantityOrginal) - SUM(OrderQuantityOriginal - CancelledQuantityOriginal - DeliveredQuantityOriginal)) AS qty_discrepancy
FROM s_shared.sales_orders;