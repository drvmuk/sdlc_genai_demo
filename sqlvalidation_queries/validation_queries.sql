-- Validation Query 1: Check if intermediate table was created successfully
%sql
SELECT COUNT(*) AS record_count
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation Query 2: Check if main table was created successfully
%sql
SELECT COUNT(*) AS record_count
FROM s_shared.sales_orders;

-- Validation Query 3: Verify key columns are populated in the intermediate table
%sql
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
    SUM(CASE WHEN MaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers,
    SUM(CASE WHEN ShipToNumber IS NULL THEN 1 ELSE 0 END) AS null_ship_to_numbers
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation Query 4: Verify key columns are populated in the main table
%sql
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
    SUM(CASE WHEN MaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers,
    SUM(CASE WHEN ShipToNumber IS NULL THEN 1 ELSE 0 END) AS null_ship_to_numbers,
    SUM(CASE WHEN SourceSystem IS NULL THEN 1 ELSE 0 END) AS null_source_system
FROM s_shared.sales_orders;

-- Validation Query 5: Check date filtering is working correctly
%sql
SELECT 
    YEAR(CreateDate) AS order_year,
    COUNT(*) AS record_count
FROM s_shared.sales_orders
GROUP BY YEAR(CreateDate)
ORDER BY order_year;

-- Validation Query 6: Verify join with currency conversion table
%sql
SELECT 
    CurrencyType,
    COUNT(*) AS record_count,
    COUNT(DISTINCT ExchangeRate) AS distinct_exchange_rates,
    MIN(ExchangeRate) AS min_exchange_rate,
    MAX(ExchangeRate) AS max_exchange_rate
FROM s_shared.sales_orders
GROUP BY CurrencyType
ORDER BY record_count DESC;

-- Validation Query 7: Check SiteId population logic
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

-- Validation Query 8: Check quantity calculations
%sql
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN OrderQuantityOriginal = OrderQuantityBase THEN 1 ELSE 0 END) AS matching_quantities,
    SUM(CASE WHEN OpenQuantityOrginal = (OrderQuantityOriginal - CancelledQuantityOriginal - DeliveredQuantityOriginal) 
        THEN 1 ELSE 0 END) AS correct_open_qty_calc
FROM s_shared.sales_orders;