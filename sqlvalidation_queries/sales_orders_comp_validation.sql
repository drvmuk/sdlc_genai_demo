-- Validation query for sales_orders_comp_stg
%sql
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CompOrderNumber) AS distinct_orders,
    COUNT(DISTINCT CompMaterialNumber) AS distinct_materials,
    SUM(CompOrderQuantityOriginal) AS total_order_qty,
    SUM(CompOpenQuantityOrginal) AS total_open_qty,
    MIN(CompCreateDate) AS earliest_order_date,
    MAX(CompCreateDate) AS latest_order_date
FROM 
    sales_orders_comp_stg;

-- Validation query for sales_orders_comp
%sql
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CompOrderNumber) AS distinct_orders,
    COUNT(DISTINCT CompMaterialNumber) AS distinct_materials,
    COUNT(DISTINCT CompSiteId) AS distinct_sites,
    SUM(CompOrderQuantityOriginal) AS total_order_qty,
    SUM(CompOpenQuantityOrginal) AS total_open_qty,
    MIN(CompCreateDate) AS earliest_order_date,
    MAX(CompCreateDate) AS latest_order_date,
    COUNT(CASE WHEN CompShipToName IS NOT NULL THEN 1 END) AS orders_with_ship_to_name,
    COUNT(CASE WHEN CompActualShipDate IS NOT NULL THEN 1 END) AS orders_with_ship_date
FROM 
    sales_orders_comp;

-- Validation query to check join quality
%sql
SELECT 
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN CompSiteId IS NOT NULL THEN 1 END) AS orders_with_site,
    COUNT(CASE WHEN CompShipToName IS NOT NULL THEN 1 END) AS orders_with_ship_to_name,
    COUNT(CASE WHEN CompSoldToName IS NOT NULL THEN 1 END) AS orders_with_sold_to_name,
    COUNT(CASE WHEN CompPayerName IS NOT NULL THEN 1 END) AS orders_with_payer_name,
    COUNT(CASE WHEN CompSalesDistributionChannel IS NOT NULL THEN 1 END) AS orders_with_dist_channel,
    COUNT(CASE WHEN CompExchangeRate IS NOT NULL THEN 1 END) AS orders_with_exchange_rate,
    COUNT(CASE WHEN CompActualShipDate IS NOT NULL THEN 1 END) AS orders_with_ship_date,
    COUNT(CASE WHEN CompLatestProcessingDate IS NOT NULL THEN 1 END) AS orders_with_processing_date,
    COUNT(CASE WHEN CompBoScope IS NOT NULL THEN 1 END) AS orders_with_bo_scope,
    COUNT(CASE WHEN CompAvailabilityDescription IS NOT NULL THEN 1 END) AS orders_with_availability_desc,
    COUNT(CASE WHEN CompBillToMarket IS NOT NULL THEN 1 END) AS orders_with_bill_to_market
FROM 
    sales_orders_comp;

-- Validation query to check for NULL values in key fields
%sql
SELECT 
    COUNT(CASE WHEN CompOrderNumber IS NULL THEN 1 END) AS null_order_number,
    COUNT(CASE WHEN CompOrderType IS NULL THEN 1 END) AS null_order_type,
    COUNT(CASE WHEN CompSalesOrgCompanyCode IS NULL THEN 1 END) AS null_company_code,
    COUNT(CASE WHEN CompLineNumber IS NULL THEN 1 END) AS null_line_number,
    COUNT(CASE WHEN CompMaterialNumber IS NULL THEN 1 END) AS null_material_number,
    COUNT(CASE WHEN CompOrderQuantityOriginal IS NULL THEN 1 END) AS null_order_qty,
    COUNT(CASE WHEN CompOpenQuantityOrginal IS NULL THEN 1 END) AS null_open_qty,
    COUNT(CASE WHEN CompCreateDate IS NULL THEN 1 END) AS null_create_date,
    COUNT(CASE WHEN CompCurrencyType IS NULL THEN 1 END) AS null_currency
FROM 
    sales_orders_comp;

-- Validation query to check date ranges
%sql
SELECT 
    MIN(CompCreateDate) AS min_create_date,
    MAX(CompCreateDate) AS max_create_date,
    MIN(CompPromisedDeliveryDate) AS min_promised_date,
    MAX(CompPromisedDeliveryDate) AS max_promised_date,
    MIN(CompRequestedDeliveryDate) AS min_requested_date,
    MAX(CompRequestedDeliveryDate) AS max_requested_date,
    MIN(CompActualShipDate) AS min_ship_date,
    MAX(CompActualShipDate) AS max_ship_date,
    MIN(CompLatestProcessingDate) AS min_processing_date,
    MAX(CompLatestProcessingDate) AS max_processing_date
FROM 
    sales_orders_comp;