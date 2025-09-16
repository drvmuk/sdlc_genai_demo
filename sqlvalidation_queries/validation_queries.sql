%sql
-- Validate the sales_orders_comp_stg table
SELECT COUNT(*) AS row_count
FROM sales_orders_comp_stg;

-- Check for null values in key fields
%sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_numbers,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers
FROM sales_orders_comp_stg;

-- Validate the sales_orders_comp table
%sql
SELECT COUNT(*) AS row_count
FROM sales_orders_comp;

-- Check for null values in key fields
%sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
    SUM(CASE WHEN CompLineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_numbers,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers
FROM sales_orders_comp;

-- Verify date transformations
%sql
SELECT 
    MIN(CompCreateDate) AS min_create_date,
    MAX(CompCreateDate) AS max_create_date,
    MIN(CompPromisedDeliveryDate) AS min_promised_date,
    MAX(CompPromisedDeliveryDate) AS max_promised_date
FROM sales_orders_comp;

-- Check for data consistency between the two tables
%sql
SELECT 
    'Matching records' AS check_type,
    COUNT(*) AS count
FROM sales_orders_comp a
JOIN sales_orders_comp_stg b
ON a.CompOrderNumber = b.CompOrderNumber
AND a.CompLineNumber = b.CompLineNumber;

-- Verify the filter for records from the last 3 years is applied
%sql
SELECT 
    YEAR(CompCreateDate) AS order_year,
    COUNT(*) AS record_count
FROM sales_orders_comp
GROUP BY YEAR(CompCreateDate)
ORDER BY order_year;