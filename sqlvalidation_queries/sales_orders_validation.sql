%sql
-- Validation Query 1: Check record counts in source and target tables
SELECT 
    'b_lots.plooh' AS table_name,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN DATA_OPERATION <> 'D' THEN 1 END) AS valid_records
FROM 
    b_lots.plooh
UNION ALL
SELECT 
    'b_lots.plool' AS table_name,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN DATA_OPERATION <> 'D' THEN 1 END) AS valid_records
FROM 
    b_lots.plool
UNION ALL
SELECT 
    's_shared.sales_orders_hdr_itm_lots' AS table_name,
    COUNT(*) AS total_records,
    COUNT(*) AS valid_records
FROM 
    s_shared.sales_orders_hdr_itm_lots
UNION ALL
SELECT 
    'sales_orders' AS table_name,
    COUNT(*) AS total_records,
    COUNT(*) AS valid_records
FROM 
    sales_orders;

%sql
-- Validation Query 2: Check for null values in key columns of intermediate table
SELECT 
    COUNT(*) AS total_records,
    COUNT(CASE WHEN SALES_ORDER_NUMBER IS NULL THEN 1 END) AS null_sales_order_number,
    COUNT(CASE WHEN SALES_ORDER_ITEM IS NULL THEN 1 END) AS null_sales_order_item,
    COUNT(CASE WHEN MATERIAL_NUMBER IS NULL THEN 1 END) AS null_material_number,
    COUNT(CASE WHEN CUSTOMER_NUMBER IS NULL THEN 1 END) AS null_customer_number
FROM 
    s_shared.sales_orders_hdr_itm_lots;

%sql
-- Validation Query 3: Check for null values in key columns of final table
SELECT 
    COUNT(*) AS total_records,
    COUNT(CASE WHEN SALES_ORDER_NUMBER IS NULL THEN 1 END) AS null_sales_order_number,
    COUNT(CASE WHEN SALES_ORDER_ITEM IS NULL THEN 1 END) AS null_sales_order_item,
    COUNT(CASE WHEN MATERIAL_NUMBER IS NULL THEN 1 END) AS null_material_number,
    COUNT(CASE WHEN CUSTOMER_NUMBER IS NULL THEN 1 END) AS null_customer_number,
    COUNT(CASE WHEN SITE_ID IS NULL THEN 1 END) AS null_site_id
FROM 
    sales_orders;

%sql
-- Validation Query 4: Check for data consistency between intermediate and final tables
SELECT 
    'Record count match' AS validation_check,
    CASE 
        WHEN (SELECT COUNT(*) FROM s_shared.sales_orders_hdr_itm_lots) = 
             (SELECT COUNT(*) FROM sales_orders)
        THEN 'PASSED'
        ELSE 'FAILED'
    END AS validation_result
UNION ALL
SELECT 
    'Sales order totals match' AS validation_check,
    CASE 
        WHEN (SELECT COUNT(DISTINCT SALES_ORDER_NUMBER) FROM s_shared.sales_orders_hdr_itm_lots) = 
             (SELECT COUNT(DISTINCT SALES_ORDER_NUMBER) FROM sales_orders)
        THEN 'PASSED'
        ELSE 'FAILED'
    END AS validation_result;

%sql
-- Validation Query 5: Check for data quality in calculated fields
SELECT 
    COUNT(*) AS total_records,
    COUNT(CASE WHEN DELIVERY_LEAD_TIME < 0 THEN 1 END) AS negative_lead_time,
    COUNT(CASE WHEN REJECTION_RATE < 0 OR REJECTION_RATE > 1 THEN 1 END) AS invalid_rejection_rate,
    COUNT(CASE WHEN ON_TIME_DELIVERY_FLAG NOT IN (0, 1) THEN 1 END) AS invalid_on_time_flag
FROM 
    sales_orders;

%sql
-- Validation Query 6: Check for currency conversion accuracy
SELECT 
    CURRENCY_CODE,
    COUNT(*) AS record_count,
    AVG(NET_VALUE) AS avg_net_value,
    AVG(NET_VALUE_USD) AS avg_net_value_usd,
    AVG(CASE WHEN CURRENCY_CODE = 'USD' THEN NET_VALUE_USD / NULLIF(NET_VALUE, 0) ELSE NULL END) AS avg_conversion_rate_usd
FROM 
    sales_orders
GROUP BY 
    CURRENCY_CODE
ORDER BY 
    CURRENCY_CODE;

%sql
-- Validation Query 7: Check for data distribution by key dimensions
SELECT 
    SALES_ORDER_TYPE,
    COUNT(*) AS record_count,
    COUNT(DISTINCT SALES_ORDER_NUMBER) AS order_count,
    SUM(NET_VALUE_USD) AS total_net_value_usd
FROM 
    sales_orders
GROUP BY 
    SALES_ORDER_TYPE
ORDER BY 
    record_count DESC;

%sql
-- Validation Query 8: Check for data distribution by time
SELECT 
    DATE_TRUNC('month', SALES_ORDER_DATE) AS order_month,
    COUNT(*) AS record_count,
    COUNT(DISTINCT SALES_ORDER_NUMBER) AS order_count,
    SUM(NET_VALUE_USD) AS total_net_value_usd
FROM 
    sales_orders
GROUP BY 
    DATE_TRUNC('month', SALES_ORDER_DATE)
ORDER BY 
    order_month;

%sql
-- Validation Query 9: Check for orphaned records in the final table
SELECT 
    'Orders without site info' AS check_type,
    COUNT(*) AS record_count
FROM 
    sales_orders
WHERE 
    SITE_ID IS NULL
UNION ALL
SELECT 
    'Orders without campaign info' AS check_type,
    COUNT(*) AS record_count
FROM 
    sales_orders
WHERE 
    CAMPAIGN_ID IS NULL
UNION ALL
SELECT 
    'Orders without payment info' AS check_type,
    COUNT(*) AS record_count
FROM 
    sales_orders
WHERE 
    PAYMENT_METHOD IS NULL
UNION ALL
SELECT 
    'Orders without hierarchy info' AS check_type,
    COUNT(*) AS record_count
FROM 
    sales_orders
WHERE 
    PRODUCT_HIERARCHY_LEVEL_1 IS NULL;

%sql
-- Validation Query 10: Check for data freshness
SELECT 
    MIN(SALES_ORDER_DATE) AS oldest_order_date,
    MAX(SALES_ORDER_DATE) AS newest_order_date,
    DATEDIFF(CURRENT_DATE(), MAX(SALES_ORDER_DATE)) AS days_since_latest_order,
    MIN(ETL_CREATED_DATE) AS oldest_etl_date,
    MAX(ETL_CREATED_DATE) AS newest_etl_date
FROM 
    sales_orders;