%sql
-- Validate sales_orders_comp_stg table creation
SELECT COUNT(*) AS row_count FROM sales_orders_comp_stg;

%sql
-- Validate sales_orders_comp_stg data - sample first 10 rows
SELECT * FROM sales_orders_comp_stg LIMIT 10;

%sql
-- Check for null values in key fields of sales_orders_comp_stg
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_numbers
FROM sales_orders_comp_stg;

%sql
-- Validate sales_orders_comp table creation
SELECT COUNT(*) AS row_count FROM sales_orders_comp;

%sql
-- Validate sales_orders_comp data - sample first 10 rows
SELECT * FROM sales_orders_comp LIMIT 10;

%sql
-- Check for null values in key fields of sales_orders_comp
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_numbers
FROM sales_orders_comp;

%sql
-- Validate date transformations
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompCreateDate IS NULL THEN 0 ELSE 1 END) AS valid_create_dates,
    SUM(CASE WHEN CompPromisedDeliveryDate IS NULL THEN 0 ELSE 1 END) AS valid_promised_delivery_dates,
    SUM(CASE WHEN CompRequestedDeliveryDate IS NULL THEN 0 ELSE 1 END) AS valid_requested_delivery_dates
FROM sales_orders_comp;

%sql
-- Validate joins worked correctly by checking for orphaned records
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompShipToName IS NULL THEN 1 ELSE 0 END) AS missing_ship_to_names,
    SUM(CASE WHEN CompSoldToName IS NULL THEN 1 ELSE 0 END) AS missing_sold_to_names,
    SUM(CASE WHEN CompPayerName IS NULL THEN 1 ELSE 0 END) AS missing_payer_names
FROM sales_orders_comp;

%sql
-- Validate data type conversions for numeric fields
SELECT 
    AVG(CompOrderQuantityOriginal) AS avg_order_qty,
    MAX(CompOrderQuantityOriginal) AS max_order_qty,
    MIN(CompOrderQuantityOriginal) AS min_order_qty,
    AVG(CompTotalPriceLocal) AS avg_price,
    MAX(CompTotalPriceLocal) AS max_price,
    MIN(CompTotalPriceLocal) AS min_price
FROM sales_orders_comp;

%sql
-- Check for data consistency between the two tables
SELECT 
    'sales_orders_comp_stg' AS table_name,
    COUNT(DISTINCT CompOrderNumber) AS distinct_orders,
    COUNT(DISTINCT CompMaterialNumber) AS distinct_materials
FROM sales_orders_comp_stg
UNION ALL
SELECT 
    'sales_orders_comp' AS table_name,
    COUNT(DISTINCT CompOrderNumber) AS distinct_orders,
    COUNT(DISTINCT CompMaterialNumber) AS distinct_materials
FROM sales_orders_comp;