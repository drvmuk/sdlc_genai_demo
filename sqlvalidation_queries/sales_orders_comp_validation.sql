-- Validation query for sales_orders_comp_stg
%sql
SELECT COUNT(*) AS row_count_stg
FROM sales_orders_comp_stg;

-- Validation query to check for null values in key fields of sales_orders_comp_stg
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

-- Validation query to check for null values in key fields of sales_orders_comp
%sql
SELECT 
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number,
    SUM(CASE WHEN CompSiteId IS NULL THEN 1 ELSE 0 END) AS null_site_id
FROM sales_orders_comp;

-- Validation query to check date fields in sales_orders_comp
%sql
SELECT 
    MIN(CompCreateDate) AS min_create_date,
    MAX(CompCreateDate) AS max_create_date,
    MIN(CompPromisedDeliveryDate) AS min_promised_delivery_date,
    MAX(CompPromisedDeliveryDate) AS max_promised_delivery_date,
    MIN(CompRequestedDeliveryDate) AS min_requested_delivery_date,
    MAX(CompRequestedDeliveryDate) AS max_requested_delivery_date,
    MIN(CompActualShipDate) AS min_actual_ship_date,
    MAX(CompActualShipDate) AS max_actual_ship_date
FROM sales_orders_comp;

-- Validation query to check numeric fields in sales_orders_comp
%sql
SELECT 
    AVG(CompOrderQuantityOriginal) AS avg_order_qty,
    MAX(CompOrderQuantityOriginal) AS max_order_qty,
    AVG(CompTotalPriceLocal) AS avg_total_price,
    MAX(CompTotalPriceLocal) AS max_total_price,
    AVG(CompUnitPriceLocal) AS avg_unit_price,
    MAX(CompUnitPriceLocal) AS max_unit_price
FROM sales_orders_comp;

-- Validation query to check the 3-year filter is working correctly
%sql
SELECT 
    YEAR(CompCreateDate) AS create_year,
    COUNT(*) AS record_count
FROM sales_orders_comp
GROUP BY YEAR(CompCreateDate)
ORDER BY create_year;

-- Validation query to check join completeness between sales_orders_comp_stg and sales_orders_comp
%sql
SELECT 
    COUNT(DISTINCT stg.CompOrderNumber) AS stg_distinct_orders,
    COUNT(DISTINCT main.CompOrderNumber) AS main_distinct_orders,
    COUNT(DISTINCT stg.CompOrderNumber) - COUNT(DISTINCT main.CompOrderNumber) AS difference
FROM 
    sales_orders_comp_stg stg
LEFT JOIN 
    sales_orders_comp main ON stg.CompOrderNumber = main.CompOrderNumber;