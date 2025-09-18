-- Validation query 1: Check if sales_orders_comp_stg was created and has data
%sql
SELECT COUNT(*) AS row_count FROM sales_orders_comp_stg;

-- Validation query 2: Check if sales_orders_comp was created and has data
%sql
SELECT COUNT(*) AS row_count FROM sales_orders_comp;

-- Validation query 3: Sample data from sales_orders_comp_stg
%sql
SELECT 
    CompOrderNumber,
    CompOrderType,
    CompSalesOrgCompanyCode,
    CompLineNumber,
    CompShipToNumber,
    CompSoldToNumber,
    CompMaterialNumber,
    CompOrderStatusLineLast,
    CompOrderStatusHeader
FROM sales_orders_comp_stg
LIMIT 10;

-- Validation query 4: Sample data from sales_orders_comp
%sql
SELECT 
    CompSourceSystem,
    CompOrderNumber,
    CompOrderType,
    CompSalesOrgCompanyCode,
    CompLineNumber,
    CompSiteId,
    CompShipToNumber,
    CompSoldToNumber,
    CompMaterialNumber
FROM sales_orders_comp
LIMIT 10;

-- Validation query 5: Check for NULL values in key fields of sales_orders_comp
%sql
SELECT 
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number,
    SUM(CASE WHEN CompShipToNumber IS NULL THEN 1 ELSE 0 END) AS null_ship_to_number,
    SUM(CASE WHEN CompSoldToNumber IS NULL THEN 1 ELSE 0 END) AS null_sold_to_number
FROM sales_orders_comp;

-- Validation query 6: Verify date transformations
%sql
SELECT 
    COUNT(*) AS total_rows,
    COUNT(CompCreateDate) AS create_date_count,
    COUNT(CompPromisedDeliveryDate) AS promised_delivery_date_count,
    COUNT(CompRequestedDeliveryDate) AS requested_delivery_date_count,
    COUNT(CompActualShipDate) AS actual_ship_date_count
FROM sales_orders_comp;

-- Validation query 7: Verify quantity calculations
%sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompOpenQuantityOrginal = (CompOrderQuantityOriginal - CompCancelledQuantityOriginal - CompDeliveredQuantityOriginal) 
        THEN 1 ELSE 0 END) AS correct_open_quantity_calc
FROM sales_orders_comp;

-- Validation query 8: Check join completeness
%sql
SELECT 
    COUNT(*) AS total_orders_in_source,
    (SELECT COUNT(*) FROM sales_orders_comp) AS total_orders_in_target,
    (SELECT COUNT(*) FROM sales_orders_comp_stg) AS total_orders_in_staging
FROM s_master.sale_orders;

-- Validation query 9: Check for duplicate order numbers and line numbers
%sql
SELECT 
    CompOrderNumber, 
    CompLineNumber, 
    COUNT(*) AS count
FROM sales_orders_comp
GROUP BY CompOrderNumber, CompLineNumber
HAVING COUNT(*) > 1
LIMIT 10;

-- Validation query 10: Check exchange rate application
%sql
SELECT 
    CompCurrencyType,
    CompExchangeRate,
    COUNT(*) AS count
FROM sales_orders_comp
GROUP BY CompCurrencyType, CompExchangeRate
ORDER BY CompCurrencyType;