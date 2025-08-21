-- Validate the intermediate table creation
%sql
SELECT COUNT(*) AS row_count
FROM s_shared.sales_orders_hdr_itm_lots;

-- Check for null values in key columns of intermediate table
%sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN LineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN MaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validate the main table creation
%sql
SELECT COUNT(*) AS row_count
FROM s_shared.sales_orders;

-- Check for null values in key columns of main table
%sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN OrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_number,
    SUM(CASE WHEN LineNumber IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN MaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_number
FROM s_shared.sales_orders;

-- Validate the date filtering is working correctly
%sql
SELECT 
    YEAR(CreateDate) AS year,
    COUNT(*) AS row_count
FROM s_shared.sales_orders
GROUP BY YEAR(CreateDate)
ORDER BY YEAR(CreateDate);

-- Validate the SiteId logic is working correctly
%sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN SiteId IS NOT NULL THEN 1 ELSE 0 END) AS non_null_site_id,
    SUM(CASE 
        WHEN SiteId IS NOT NULL AND 
             EXISTS (
                SELECT 1 FROM site_data e 
                WHERE TRIM(OrderNumber) = TRIM(CONCAT(e.refto, e.refno))
                AND dreqlc = e.dreql
                AND LineNumber = e.seqnl
                AND TRIM(exttx) = TRIM(e.exttx)
                AND seqne = e.seqne
                AND TRIM(e.whcda) <> ''
                AND e.whcda IS NOT NULL
             )
        THEN 1 ELSE 0 END) AS site_id_from_site_data
FROM s_shared.sales_orders;

-- Validate the currency exchange rate logic
%sql
SELECT 
    CurrencyType,
    COUNT(*) AS row_count,
    AVG(ExchangeRate) AS avg_exchange_rate
FROM s_shared.sales_orders
GROUP BY CurrencyType
ORDER BY CurrencyType;

-- Validate the date transformations
%sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CreateDate IS NOT NULL THEN 1 ELSE 0 END) AS non_null_create_date,
    SUM(CASE WHEN PromisedDeliveryDate IS NOT NULL THEN 1 ELSE 0 END) AS non_null_promised_delivery_date,
    SUM(CASE WHEN RequestedDeliveryDate IS NOT NULL THEN 1 ELSE 0 END) AS non_null_requested_delivery_date,
    SUM(CASE WHEN ActualShipDate IS NOT NULL THEN 1 ELSE 0 END) AS non_null_actual_ship_date
FROM s_shared.sales_orders;