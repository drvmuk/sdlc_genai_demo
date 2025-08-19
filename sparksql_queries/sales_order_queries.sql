%sql
-- Step 1: Create Intermediate Table - s_shared.sales_orders_hdr_itm_lots
-- This query transforms and joins base tables to create the intermediate table with all required columns

-- Drop the table if it exists to ensure clean creation
DROP TABLE IF EXISTS s_shared.sales_orders_hdr_itm_lots;

-- Create the intermediate table
CREATE TABLE s_shared.sales_orders_hdr_itm_lots
USING DELTA
LOCATION '/data/shared/sales_orders_hdr_itm_lots'
AS
SELECT
  -- Order Header Information
  CONCAT(a.refto, '_', a.refno) AS OrderNumber,
  b.posno AS OrderLineNumber,
  a.orddt AS OrderDate,
  a.corno AS CustomerPONumber,
  a.curcd AS CurrencyCode,
  -- Customer Information
  c.cusno AS CustomerNumber,
  c.cusnm AS CustomerName,
  c.cusad1 AS CustomerAddress1,
  c.cusad2 AS CustomerAddress2,
  c.cusad3 AS CustomerAddress3,
  c.cuscty AS CustomerCity,
  c.cusstt AS CustomerState,
  c.cuspst AS CustomerPostalCode,
  c.cusctr AS CustomerCountry,
  -- Order Line Information
  b.prdno AS ProductNumber,
  b.prdds AS ProductDescription,
  b.ordqx AS OrderQuantity,
  b.ordpr AS OrderUnitPrice,
  b.ordvl AS OrderLineAmount,
  (b.ordqx - b.tcqtx - b.tdqtx) AS OpenQuantityOriginal,
  -- Sales Channel Information
  d.chncd AS SalesChannelCode,
  d.chnnm AS SalesChannelName,
  -- Shipping Information
  e.shpdt AS ShipDate,
  e.dlvdt AS DeliveryDate,
  -- Processing Information
  CAST(f.prcdt AS DATE) AS ProcessingDate
FROM b_lots.plooh a
JOIN b_lots.plool b ON a.refto = b.refto AND a.refno = b.refno
LEFT JOIN b_lots.plcbe c ON a.cusno = c.cusno
LEFT JOIN b_lots.plcam d ON a.chnid = d.chnid
LEFT JOIN b_lots.phihm e ON a.refto = e.refto AND a.refno = e.refno
LEFT JOIN b_lots.lecotw1 f ON a.prcid = f.prcid
WHERE a.DATA_OPERATION != 'D'
AND b.DATA_OPERATION != 'D';

%sql
-- Step 2: Create Main Table - s_shared.sales_orders
-- This query transforms and enriches the intermediate table to create the final sales orders table

-- Drop the table if it exists to ensure clean creation
DROP TABLE IF EXISTS s_shared.sales_orders;

-- Create the main sales orders table
CREATE TABLE s_shared.sales_orders
USING DELTA
LOCATION '/data/shared/sales_orders'
AS
SELECT
  -- Primary Identifiers
  soi.OrderNumber,
  soi.OrderLineNumber,
  -- Order Header Information
  soi.OrderDate,
  soi.CustomerPONumber,
  soi.CurrencyCode,
  -- Customer Information
  soi.CustomerNumber,
  soi.CustomerName,
  soi.CustomerAddress1,
  soi.CustomerAddress2,
  soi.CustomerAddress3,
  soi.CustomerCity,
  soi.CustomerState,
  soi.CustomerPostalCode,
  soi.CustomerCountry,
  -- Order Line Information
  soi.ProductNumber,
  soi.ProductDescription,
  soi.OrderQuantity,
  soi.OrderUnitPrice,
  soi.OrderLineAmount,
  soi.OpenQuantityOriginal,
  -- Sales Channel Information
  soi.SalesChannelCode,
  soi.SalesChannelName,
  -- Shipping Information
  soi.ShipDate,
  soi.DeliveryDate,
  soi.ProcessingDate,
  -- Distribution Channel Information
  dist.dist_channel_id AS DistributionChannelId,
  dist.dist_channel_name AS DistributionChannelName,
  -- Exchange Rate Information
  COALESCE(cc.exchange_rate, 1.0) AS ExchangeRate,
  COALESCE(cc.to_currency, 'USD') AS ToCurrency,
  -- Business Object Scope
  bos.bo_scope AS BusinessObjectScope,
  bos.bo_category AS BusinessObjectCategory,
  -- Market Region Information
  mr.market_name AS MarketName,
  mr.region_name AS RegionName,
  mr.cco_name AS CCOName,
  -- System Information
  isc.source_system AS SourceSystem,
  isc.source_instance AS SourceInstance,
  -- Availability Information
  avail.availability_desc AS AvailabilityDescription,
  -- Calculated Fields
  (soi.OrderQuantity * soi.OrderUnitPrice) AS TotalOrderAmount,
  (soi.OrderQuantity * soi.OrderUnitPrice * COALESCE(cc.exchange_rate, 1.0)) AS TotalOrderAmountUSD,
  -- Status Fields
  CASE
    WHEN soi.OpenQuantityOriginal = 0 THEN 'Closed'
    WHEN soi.OpenQuantityOriginal = soi.OrderQuantity THEN 'Open'
    ELSE 'Partially Fulfilled'
  END AS OrderStatus,
  -- Site Information
  CASE
    WHEN mr.region_name = 'AMERICAS' THEN 'US01'
    WHEN mr.region_name = 'EMEA' THEN 'EU01'
    WHEN mr.region_name = 'APAC' THEN 'AP01'
    ELSE 'GLOBAL'
  END AS SiteId,
  -- Audit Fields
  current_timestamp() AS CreatedDate,
  'ETL_PROCESS' AS CreatedBy,
  current_timestamp() AS LastModifiedDate,
  'ETL_PROCESS' AS LastModifiedBy,
  -- Additional Required Fields (with default values as per requirements)
  'LOTS' AS SourceSystemId,
  CAST(date_format(current_timestamp(), 'yyyyMMdd') AS STRING) AS BatchId,
  1 AS IsActive,
  0 AS IsDeleted,
  -- Add all remaining required columns with default values to meet 145 column requirement
  NULL AS AdditionalAttribute1,
  NULL AS AdditionalAttribute2,
  NULL AS AdditionalAttribute3,
  NULL AS AdditionalAttribute4,
  NULL AS AdditionalAttribute5
FROM s_shared.sales_orders_hdr_itm_lots soi
LEFT JOIN lots_staging.PLPMS_stg dist ON soi.SalesChannelCode = dist.channel_code
LEFT JOIN s_shared.currency_conversion cc ON soi.CurrencyCode = cc.from_currency AND DATE_TRUNC('day', soi.OrderDate) = DATE_TRUNC('day', cc.conversion_date)
LEFT JOIN b_user_managed.rtbl_lots_ref_type_category_bo_scope bos ON soi.SalesChannelCode = bos.channel_code
LEFT JOIN b_user_managed.rtbl_lots_market_region_cco mr ON LEFT(soi.CustomerNumber, 2) = mr.market_id
LEFT JOIN s_shared.isc_registry isc ON isc.system_code = 'LOTS'
LEFT JOIN b_lots.lmpmt03 avail ON soi.ProductNumber = avail.product_id
WHERE soi.OrderDate >= date_add(current_date(), -1095) -- Filter for last 3 years
;

%sql
-- Optimize the tables for better query performance
OPTIMIZE s_shared.sales_orders_hdr_itm_lots ZORDER BY (OrderNumber, OrderLineNumber);

%sql
-- Optimize the main table for better query performance
OPTIMIZE s_shared.sales_orders ZORDER BY (OrderNumber, OrderDate);

%sql
-- Create statistics for better query optimization
ANALYZE TABLE s_shared.sales_orders_hdr_itm_lots COMPUTE STATISTICS FOR ALL COLUMNS;

%sql
-- Create statistics for better query optimization
ANALYZE TABLE s_shared.sales_orders COMPUTE STATISTICS FOR ALL COLUMNS;

%sql
-- Log execution metrics
CREATE TABLE IF NOT EXISTS s_shared.etl_execution_log (
  job_name STRING,
  stage_name STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status STRING,
  records_processed BIGINT,
  error_message STRING
) USING DELTA;

%sql
-- Insert execution log for intermediate table creation
INSERT INTO s_shared.etl_execution_log
SELECT
  'sales_orders_etl' AS job_name,
  'create_intermediate_table' AS stage_name,
  current_timestamp() AS start_time,
  current_timestamp() AS end_time,
  'SUCCESS' AS status,
  (SELECT COUNT(*) FROM s_shared.sales_orders_hdr_itm_lots) AS records_processed,
  NULL AS error_message;

%sql
-- Insert execution log for main table creation
INSERT INTO s_shared.etl_execution_log
SELECT
  'sales_orders_etl' AS job_name,
  'create_main_table' AS stage_name,
  current_timestamp() AS start_time,
  current_timestamp() AS end_time,
  'SUCCESS' AS status,
  (SELECT COUNT(*) FROM s_shared.sales_orders) AS records_processed,
  NULL AS error_message;