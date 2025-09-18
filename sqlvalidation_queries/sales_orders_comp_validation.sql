%sql
-- Validation query for sales_orders_comp_stg
SELECT COUNT(*) AS row_count_stg FROM sales_orders_comp_stg;

%sql
-- Check for null values in key columns of sales_orders_comp_stg
SELECT 
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number
FROM sales_orders_comp_stg;

%sql
-- Validation query for sales_orders_comp
SELECT COUNT(*) AS row_count_main FROM sales_orders_comp;

%sql
-- Check for null values in key columns of sales_orders_comp
SELECT 
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number
FROM sales_orders_comp;

%sql
-- Check join quality between staging and main table
SELECT 
    COUNT(a.CompOrderNumber) AS stg_count,
    COUNT(b.CompOrderNumber) AS main_count,
    COUNT(CASE WHEN a.CompOrderNumber = b.CompOrderNumber THEN 1 END) AS matched_count
FROM 
    sales_orders_comp_stg a
LEFT JOIN 
    sales_orders_comp b ON a.CompOrderNumber = b.CompOrderNumber AND a.CompLineNumber = b.CompLineNumber;

%sql
-- Validate date transformations
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompCreateDate IS NULL THEN 1 ELSE 0 END) AS null_create_date,
    SUM(CASE WHEN CompPromisedDeliveryDate IS NULL THEN 1 ELSE 0 END) AS null_promised_delivery_date,
    SUM(CASE WHEN CompRequestedDeliveryDate IS NULL THEN 1 ELSE 0 END) AS null_requested_delivery_date
FROM sales_orders_comp;

%sql
-- Validate currency and price data
SELECT 
    CompCurrencyType,
    COUNT(*) AS row_count,
    AVG(CompTotalPriceLocal) AS avg_price,
    MIN(CompTotalPriceLocal) AS min_price,
    MAX(CompTotalPriceLocal) AS max_price
FROM sales_orders_comp
GROUP BY CompCurrencyType
ORDER BY row_count DESC;

%sql
-- Validate quantity calculations
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompOpenQuantityOrginal = (CompOrderQuantityOriginal - CompCancelledQuantityOriginal - CompDeliveredQuantityOriginal) THEN 1 ELSE 0 END) AS correct_calc_count
FROM sales_orders_comp;