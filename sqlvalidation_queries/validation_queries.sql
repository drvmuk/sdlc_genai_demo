%sql
-- Validation query for sales_orders_comp_stg table
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CompOrderNumber) AS distinct_orders,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_count,
    SUM(CASE WHEN CompOrderQuantityOriginal IS NULL THEN 1 ELSE 0 END) AS null_quantity_count
FROM 
    sales_orders_comp_stg;

-- Validation query for sales_orders_comp table
%sql
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CompOrderNumber) AS distinct_orders,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_count,
    SUM(CASE WHEN CompOrderQuantityOriginal IS NULL THEN 1 ELSE 0 END) AS null_quantity_count,
    SUM(CASE WHEN CompSiteId IS NULL THEN 1 ELSE 0 END) AS null_site_count,
    SUM(CASE WHEN CompShipToName IS NULL THEN 1 ELSE 0 END) AS null_ship_to_name_count
FROM 
    sales_orders_comp;

-- Validation query to check join between sales_orders_comp and source tables
%sql
SELECT 
    a.CompOrderNumber,
    a.CompMaterialNumber,
    b.MaterialNumber AS source_material_number,
    a.CompOrderQuantityOriginal,
    b.OrderQuantityOriginal AS source_quantity
FROM 
    sales_orders_comp a
JOIN 
    s_master.sale_orders b
ON 
    a.CompOrderNumber = b.OrderNumber
    AND a.CompLineNumber = b.LineNumber
LIMIT 100;

-- Validation query to check date transformations
%sql
SELECT 
    CompOrderNumber,
    CompCreateDate,
    CompPromisedDeliveryDate,
    CompRequestedDeliveryDate,
    CompActualShipDate,
    CompLatestProcessingDate
FROM 
    sales_orders_comp
WHERE 
    CompCreateDate IS NOT NULL
LIMIT 100;

-- Validation query to check currency and price calculations
%sql
SELECT 
    CompOrderNumber,
    CompCurrencyType,
    CompExchangeRate,
    CompTotalPriceLocal,
    CompUnitPriceLocal
FROM 
    sales_orders_comp
WHERE 
    CompTotalPriceLocal IS NOT NULL
LIMIT 100;