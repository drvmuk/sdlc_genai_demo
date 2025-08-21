-- Validation query for sales_orders_comp_stg
%sql
SELECT COUNT(*) AS row_count_stg
FROM sales_orders_comp_stg;

-- Check for null values in key fields of sales_orders_comp_stg
%sql
SELECT 
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number
FROM sales_orders_comp_stg;

-- Validation query for sales_orders_comp
%sql
SELECT COUNT(*) AS row_count_main
FROM sales_orders_comp;

-- Check for data integrity between staging and main tables
%sql
SELECT 
    COUNT(stg.CompOrderNumber) AS stg_orders,
    COUNT(main.CompOrderNumber) AS main_orders,
    COUNT(CASE WHEN stg.CompOrderNumber = main.CompOrderNumber THEN 1 END) AS matched_orders
FROM 
    sales_orders_comp_stg stg
LEFT JOIN 
    sales_orders_comp main 
ON 
    stg.CompOrderNumber = main.CompOrderNumber
    AND stg.CompLineNumber = main.CompLineNumber;

-- Check for date field conversions
%sql
SELECT 
    MIN(CompCreateDate) AS min_create_date,
    MAX(CompCreateDate) AS max_create_date,
    MIN(CompPromisedDeliveryDate) AS min_promised_date,
    MAX(CompPromisedDeliveryDate) AS max_promised_date,
    MIN(CompRequestedDeliveryDate) AS min_requested_date,
    MAX(CompRequestedDeliveryDate) AS max_requested_date
FROM sales_orders_comp;

-- Check for currency exchange rate application
%sql
SELECT 
    CompCurrencyType,
    AVG(CompExchangeRate) AS avg_exchange_rate,
    COUNT(*) AS record_count
FROM 
    sales_orders_comp
GROUP BY 
    CompCurrencyType
ORDER BY 
    record_count DESC;

-- Check for join success with site information
%sql
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN CompSiteId IS NOT NULL THEN 1 ELSE 0 END) AS records_with_site_id,
    (SUM(CASE WHEN CompSiteId IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_with_site_id
FROM 
    sales_orders_comp;

-- Check for distribution channel mapping
%sql
SELECT 
    CompSalesDistributionChannel,
    COUNT(*) AS record_count
FROM 
    sales_orders_comp
WHERE 
    CompSalesDistributionChannel IS NOT NULL
GROUP BY 
    CompSalesDistributionChannel
ORDER BY 
    record_count DESC
LIMIT 10;