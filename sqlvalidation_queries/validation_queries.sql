%sql
-- Validation Query 1: Check if sales_orders_comp_stg table was created and has data
SELECT COUNT(*) AS row_count FROM sales_orders_comp_stg;

-- Validation Query 2: Check if sales_orders_comp table was created and has data
SELECT COUNT(*) AS row_count FROM sales_orders_comp;

-- Validation Query 3: Check for NULL values in key fields in sales_orders_comp_stg
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number
FROM sales_orders_comp_stg;

-- Validation Query 4: Check for NULL values in key fields in sales_orders_comp
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number
FROM sales_orders_comp;

-- Validation Query 5: Check date format in sales_orders_comp_stg
SELECT 
    MIN(CompCreateDate) AS min_create_date,
    MAX(CompCreateDate) AS max_create_date,
    MIN(CompPromisedDeliveryDate) AS min_promised_date,
    MAX(CompPromisedDeliveryDate) AS max_promised_date,
    MIN(CompRequestedDeliveryDate) AS min_requested_date,
    MAX(CompRequestedDeliveryDate) AS max_requested_date
FROM sales_orders_comp_stg;

-- Validation Query 6: Check date format in sales_orders_comp
SELECT 
    MIN(CompCreateDate) AS min_create_date,
    MAX(CompCreateDate) AS max_create_date,
    MIN(CompPromisedDeliveryDate) AS min_promised_date,
    MAX(CompPromisedDeliveryDate) AS max_promised_date,
    MIN(CompRequestedDeliveryDate) AS min_requested_date,
    MAX(CompRequestedDeliveryDate) AS max_requested_date,
    MIN(CompActualShipDate) AS min_ship_date,
    MAX(CompActualShipDate) AS max_ship_date
FROM sales_orders_comp;

-- Validation Query 7: Check numeric fields in sales_orders_comp_stg
SELECT 
    MIN(CompOrderQuantityOriginal) AS min_order_qty,
    MAX(CompOrderQuantityOriginal) AS max_order_qty,
    MIN(CompTotalPriceLocal) AS min_price,
    MAX(CompTotalPriceLocal) AS max_price
FROM sales_orders_comp_stg;

-- Validation Query 8: Check numeric fields in sales_orders_comp
SELECT 
    MIN(CompOrderQuantityOriginal) AS min_order_qty,
    MAX(CompOrderQuantityOriginal) AS max_order_qty,
    MIN(CompTotalPriceLocal) AS min_price,
    MAX(CompTotalPriceLocal) AS max_price,
    MIN(CompExchangeRate) AS min_exchange_rate,
    MAX(CompExchangeRate) AS max_exchange_rate
FROM sales_orders_comp;

-- Validation Query 9: Check join integrity between source tables in sales_orders_comp_stg
SELECT 
    COUNT(*) AS total_rows_in_stg,
    COUNT(DISTINCT CompOrderNumber) AS distinct_order_numbers,
    COUNT(DISTINCT CompLineNumber) AS distinct_line_numbers
FROM sales_orders_comp_stg;

-- Validation Query 10: Check join integrity between source tables in sales_orders_comp
SELECT 
    COUNT(*) AS total_rows_in_comp,
    COUNT(DISTINCT CompOrderNumber) AS distinct_order_numbers,
    COUNT(DISTINCT CompLineNumber) AS distinct_line_numbers,
    COUNT(DISTINCT CompSiteId) AS distinct_site_ids
FROM sales_orders_comp;