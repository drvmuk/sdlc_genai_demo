%sql
-- Validate the intermediate table creation
SELECT COUNT(*) AS row_count_intermediate FROM s_shared.sales_orders_hdr_itm_lots;

-- Validate key fields in the intermediate table
SELECT 
    COUNT(DISTINCT OrderNumber) AS distinct_order_numbers,
    COUNT(DISTINCT MaterialNumber) AS distinct_materials,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN OrderQuantityOriginal IS NULL THEN 1 END) AS null_order_quantities,
    COUNT(CASE WHEN CreateDate IS NULL THEN 1 END) AS null_create_dates
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validate the main table creation
SELECT COUNT(*) AS row_count_main FROM s_shared.sales_orders;

-- Validate key fields in the main table
SELECT 
    COUNT(DISTINCT OrderNumber) AS distinct_order_numbers,
    COUNT(DISTINCT MaterialNumber) AS distinct_materials,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN SourceSystem IS NULL THEN 1 END) AS null_source_system,
    COUNT(CASE WHEN CreateDate IS NULL THEN 1 END) AS null_create_dates
FROM s_shared.sales_orders;

-- Validate the date filtering condition is working
SELECT 
    MIN(YEAR(CreateDate)) AS min_year,
    MAX(YEAR(CreateDate)) AS max_year,
    YEAR(CURRENT_DATE()) - 3 AS cutoff_year
FROM s_shared.sales_orders;

-- Validate the SiteId logic
SELECT 
    COUNT(CASE WHEN SiteId IS NOT NULL THEN 1 END) AS non_null_site_ids,
    COUNT(*) AS total_rows,
    (COUNT(CASE WHEN SiteId IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)) AS site_id_population_percentage
FROM s_shared.sales_orders;

-- Validate currency exchange rate logic
SELECT 
    CurrencyType,
    COUNT(*) AS record_count,
    AVG(ExchangeRate) AS avg_exchange_rate
FROM s_shared.sales_orders
GROUP BY CurrencyType
ORDER BY record_count DESC;

-- Validate join with plcbe for customer names
SELECT 
    COUNT(CASE WHEN ShipToName IS NOT NULL THEN 1 END) AS non_null_ship_to_names,
    COUNT(CASE WHEN SoldToName IS NOT NULL THEN 1 END) AS non_null_sold_to_names,
    COUNT(CASE WHEN PayerName IS NOT NULL THEN 1 END) AS non_null_payer_names,
    COUNT(*) AS total_rows
FROM s_shared.sales_orders;