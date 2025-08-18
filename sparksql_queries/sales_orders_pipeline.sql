%sql
-- Create intermediate table sales_orders_hdr_itm_lots
CREATE TABLE IF NOT EXISTS s_shared.sales_orders_hdr_itm_lots
USING DELTA
AS
SELECT
  -- Order header information
  CONCAT(h.refto, h.refno) AS OrderNumber,
  TO_DATE(h.orddt) AS OrderDate,
  h.cusno AS CustomerNumber,
  h.cusna AS CustomerName,
  h.cntry AS CustomerCountry,
  h.orsts AS OrderStatus,
  h.ordtp AS OrderType,
  h.curnc AS Currency,
  h.excha AS ExchangeRate,
  h.ordva AS OrderAmount,
  -- Item information
  i.itmno AS ItemNumber,
  i.itmds AS ItemDescription,
  i.itmqt AS ItemQuantity,
  i.itmpr AS ItemPrice,
  i.itmva AS ItemValue,
  i.itmst AS ItemStatus,
  -- Additional fields from header
  h.refto AS ReferenceTypeCode,
  h.refno AS ReferenceNumber,
  h.ordno AS OriginalOrderNumber,
  h.cusad AS CustomerAddress,
  h.cusat AS CustomerAttention,
  h.cusct AS CustomerCity,
  h.cuspc AS CustomerPostalCode,
  h.cusdl AS DeliveryLocation,
  h.cuspo AS CustomerPurchaseOrder,
  h.cusrf AS CustomerReference,
  h.salof AS SalesOffice,
  h.salno AS SalesNumber,
  h.salna AS SalesName,
  h.orddt AS OrderDateRaw,
  h.dlvdt AS DeliveryDate,
  h.invdt AS InvoiceDate,
  h.ordrm AS OrderRemarks,
  h.invno AS InvoiceNumber,
  h.invst AS InvoiceStatus,
  -- Additional fields from item
  i.itmln AS ItemLineNumber,
  i.itmct AS ItemCategory,
  i.itmum AS ItemUnitOfMeasure,
  i.itmcu AS ItemCurrency,
  i.itmex AS ItemExchangeRate,
  i.itmrm AS ItemRemarks
FROM b_lots.plooh h
JOIN b_lots.plool i ON h.refto = i.refto AND h.refno = i.refno
WHERE h.orsts != 'CANCELLED'
  AND i.itmst != 'CANCELLED';

%sql
-- Create main sales_orders table with additional enrichment
CREATE TABLE IF NOT EXISTS sales_orders
USING DELTA
AS
SELECT
  so.OrderNumber,
  so.OrderDate,
  so.CustomerNumber,
  so.CustomerName,
  so.CustomerCountry,
  so.OrderStatus,
  so.OrderType,
  so.Currency,
  so.ExchangeRate,
  so.OrderAmount,
  -- Convert order amount to USD
  CASE 
    WHEN so.Currency = 'USD' THEN so.OrderAmount
    ELSE COALESCE(so.OrderAmount * cc.exchange_rate, so.OrderAmount * so.ExchangeRate)
  END AS OrderAmountUSD,
  so.ItemNumber,
  so.ItemDescription,
  so.ItemQuantity,
  so.ItemPrice,
  so.ItemValue,
  so.ItemStatus,
  -- Additional enriched fields
  cbe.custp AS CustomerType,
  cbe.indcd AS IndustryCode,
  cbe.indna AS IndustryName,
  cam.camcd AS CampaignCode,
  cam.camna AS CampaignName,
  pms.pmscd AS ProductManagerCode,
  pms.pmsna AS ProductManagerName,
  -- Market region mapping
  COALESCE(mrc.market_region, 'UNKNOWN') AS market_region,
  -- Reference type category
  COALESCE(rtc.ref_type_category, 'UNKNOWN') AS ref_type_category,
  -- Business owner scope
  COALESCE(rtc.bo_scope, 'UNKNOWN') AS business_owner_scope,
  -- Additional fields from source tables
  him.hinum AS HierarchyNumber,
  him.hilvl AS HierarchyLevel,
  him.hinam AS HierarchyName,
  eco.ecocd AS EconomicCode,
  eco.econa AS EconomicName,
  lmp.lmpcd AS LastMilePartnerCode,
  lmp.lmpna AS LastMilePartnerName,
  -- Registry information
  reg.registry_id,
  reg.registry_status,
  reg.registry_date,
  -- Audit columns
  current_timestamp() AS created_at,
  'SYSTEM' AS created_by,
  current_timestamp() AS updated_at,
  'SYSTEM' AS updated_by
FROM s_shared.sales_orders_hdr_itm_lots so
-- Left joins to preserve all sales order records
LEFT JOIN b_lots.plcbe cbe ON so.CustomerNumber = cbe.cusno
LEFT JOIN b_lots.plcam cam ON so.ReferenceTypeCode = cam.refto AND so.ReferenceNumber = cam.refno
LEFT JOIN b_lots.plpms pms ON so.ItemNumber = pms.itmno
LEFT JOIN b_lots.phihm him ON so.CustomerNumber = him.cusno
LEFT JOIN b_lots.lecotw1 eco ON so.CustomerCountry = eco.cntry
LEFT JOIN b_lots.lmpmt03 lmp ON so.DeliveryLocation = lmp.dlvlc
LEFT JOIN s_shared.isc_registry reg ON so.OrderNumber = reg.order_number
LEFT JOIN s_shared.currency_conversion cc 
  ON so.Currency = cc.source_currency 
  AND 'USD' = cc.target_currency 
  AND so.OrderDate BETWEEN cc.effective_date AND cc.expiration_date
LEFT JOIN b_user_managed.rtbl_lots_ref_type_category_bo_scope rtc 
  ON so.ReferenceTypeCode = rtc.ref_type_code
LEFT JOIN b_user_managed.rtbl_lots_market_region_cco mrc 
  ON so.CustomerCountry = mrc.country_code;

%sql
-- Optimize the tables for better query performance
OPTIMIZE s_shared.sales_orders_hdr_itm_lots
ZORDER BY (OrderNumber, CustomerNumber, ItemNumber);

%sql
-- Optimize the main table
OPTIMIZE sales_orders
ZORDER BY (OrderNumber, CustomerNumber, OrderDate);

%sql
-- Add table properties for better management
ALTER TABLE s_shared.sales_orders_hdr_itm_lots
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'description' = 'Intermediate table for sales orders data pipeline'
);

%sql
-- Add table properties for better management
ALTER TABLE sales_orders
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'description' = 'Main sales orders table with enriched data'
);

%sql
-- Create a view for simplified access to the most common fields
CREATE OR REPLACE VIEW sales_orders_summary AS
SELECT
  OrderNumber,
  OrderDate,
  CustomerNumber,
  CustomerName,
  CustomerCountry,
  OrderStatus,
  OrderType,
  Currency,
  OrderAmount,
  OrderAmountUSD,
  market_region,
  ref_type_category,
  business_owner_scope,
  COUNT(DISTINCT ItemNumber) AS TotalItems,
  SUM(ItemQuantity) AS TotalQuantity,
  SUM(ItemValue) AS TotalItemValue
FROM sales_orders
GROUP BY
  OrderNumber,
  OrderDate,
  CustomerNumber,
  CustomerName,
  CustomerCountry,
  OrderStatus,
  OrderType,
  Currency,
  OrderAmount,
  OrderAmountUSD,
  market_region,
  ref_type_category,
  business_owner_scope;