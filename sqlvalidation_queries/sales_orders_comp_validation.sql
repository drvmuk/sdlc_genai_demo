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

-- Check for data consistency between staging and main tables
%sql
SELECT 
    stg.CompOrderNumber,
    stg.CompLineNumber,
    stg.CompMaterialNumber,
    main.CompOrderNumber AS main_order_number,
    main.CompLineNumber AS main_line_number,
    main.CompMaterialNumber AS main_material_number,
    CASE 
        WHEN main.CompOrderNumber IS NULL THEN 'Missing in main'
        ELSE 'Present in both'
    END AS status
FROM 
    sales_orders_comp_stg stg
LEFT JOIN 
    sales_orders_comp main 
ON 
    stg.CompOrderNumber = main.CompOrderNumber
    AND stg.CompLineNumber = main.CompLineNumber
WHERE 
    main.CompOrderNumber IS NULL
LIMIT 100;

-- Check for data quality in the main table
%sql
SELECT 
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number,
    SUM(CASE WHEN CompSiteId IS NULL THEN 1 ELSE 0 END) AS null_site_id,
    SUM(CASE WHEN CompShipToNumber IS NULL THEN 1 ELSE 0 END) AS null_ship_to_number,
    SUM(CASE WHEN CompSoldToNumber IS NULL THEN 1 ELSE 0 END) AS null_sold_to_number
FROM 
    sales_orders_comp;

-- Check date fields for validity
%sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompCreateDate > CURRENT_DATE() THEN 1 ELSE 0 END) AS future_create_dates,
    SUM(CASE WHEN CompPromisedDeliveryDate < CompCreateDate THEN 1 ELSE 0 END) AS invalid_promise_dates,
    SUM(CASE WHEN CompRequestedDeliveryDate < CompCreateDate THEN 1 ELSE 0 END) AS invalid_request_dates
FROM 
    sales_orders_comp;

-- Check numeric calculations
%sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompOpenQuantityOrginal != (CompOrderQuantityOriginal - CompCancelledQuantityOriginal - CompDeliveredQuantityOriginal) THEN 1 ELSE 0 END) AS inconsistent_quantities
FROM 
    sales_orders_comp;

-- Verify that the data is filtered correctly by year
%sql
SELECT 
    YEAR(CompCreateDate) AS order_year,
    COUNT(*) AS order_count
FROM 
    sales_orders_comp
GROUP BY 
    YEAR(CompCreateDate)
ORDER BY 
    order_year;