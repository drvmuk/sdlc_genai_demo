%sql
-- Stage 1: Retrieve and join data from b_lots.plooh and b_lots.plool
-- Create intermediate table for sales orders
CREATE OR REPLACE TABLE s_shared.sales_orders_hdr_itm_lots
USING DELTA
LOCATION '/mnt/data/s_shared/sales_orders_hdr_itm_lots'
AS
SELECT 
  h.refto AS OrderNumber,
  h.refty AS OrderType,
  h.bukrs AS SalesOrgCompanyCode,
  h.vkorg AS SalesOrganization,
  h.vtweg AS DistributionChannel,
  h.spart AS Division,
  h.auart AS SalesDocumentType,
  h.kunnr AS SoldToParty,
  h.kunag AS ShipToParty,
  h.bstkd AS CustomerPurchaseOrderNumber,
  h.bstdk AS CustomerPurchaseOrderDate,
  h.datoxc AS OrderCreationDate,
  h.audat AS DocumentDate,
  h.vdatu AS RequestedDeliveryDate,
  h.erdat AS CreatedOn,
  h.erzet AS EntryTime,
  h.ernam AS CreatedBy,
  i.posnr AS ItemNumber,
  i.matnr AS MaterialNumber,
  i.arktx AS ItemDescription,
  i.kwmeng AS OrderQuantity,
  i.vrkme AS SalesUnit,
  i.werks AS Plant,
  i.lgort AS StorageLocation,
  i.vstel AS ShippingPoint,
  i.route AS Route,
  i.lprio AS DeliveryPriority,
  i.sdabw AS SpecialProcessingIndicator,
  i.kzazu AS OrderCombinationIndicator,
  i.netwr AS NetValue,
  i.waerk AS Currency,
  h.DATA_OPERATION,
  i.DATA_OPERATION AS ITEM_DATA_OPERATION
FROM b_lots.plooh h
JOIN b_lots.plool i 
  ON h.refto = i.refto 
  AND h.refno = i.refno
WHERE h.DATA_OPERATION <> 'D';

-- Stage 4-6: Create main sales_orders table with additional joins and transformations
%sql
CREATE OR REPLACE TABLE sales_orders
USING DELTA
LOCATION '/mnt/data/sales_orders'
AS
SELECT 
  so.OrderNumber,
  so.OrderType,
  so.SalesOrgCompanyCode,
  so.SalesOrganization,
  so.DistributionChannel,
  dc.vtext AS SalesDistributionChannel,
  so.Division,
  so.SalesDocumentType,
  so.SoldToParty,
  stc.name1 AS SoldToName,
  so.ShipToParty,
  shc.name1 AS ShipToName,
  so.CustomerPurchaseOrderNumber,
  to_date(from_unixtime(unix_timestamp(cast(so.CustomerPurchaseOrderDate as string), 'yyyyMMdd'))) AS CustomerPurchaseOrderDate,
  to_date(from_unixtime(unix_timestamp(cast(so.OrderCreationDate as string), 'yyyyMMdd'))) AS OrderCreationDate,
  to_date(from_unixtime(unix_timestamp(cast(so.DocumentDate as string), 'yyyyMMdd'))) AS DocumentDate,
  to_date(from_unixtime(unix_timestamp(cast(so.RequestedDeliveryDate as string), 'yyyyMMdd'))) AS RequestedDeliveryDate,
  to_date(from_unixtime(unix_timestamp(cast(so.CreatedOn as string), 'yyyyMMdd'))) AS CreatedOn,
  so.EntryTime,
  so.CreatedBy,
  so.ItemNumber,
  so.MaterialNumber,
  so.ItemDescription,
  so.OrderQuantity,
  so.SalesUnit,
  so.Plant,
  so.StorageLocation,
  so.ShippingPoint,
  so.Route,
  so.DeliveryPriority,
  so.SpecialProcessingIndicator,
  so.OrderCombinationIndicator,
  so.NetValue,
  so.Currency,
  CASE 
    WHEN pal.werks IS NOT NULL THEN pal.werks
    WHEN psl.werks IS NOT NULL THEN psl.werks
    ELSE so.Plant
  END AS SiteId,
  current_timestamp() AS ETL_TIMESTAMP
FROM s_shared.sales_orders_hdr_itm_lots so
LEFT JOIN b_lots.plcam dc 
  ON so.DistributionChannel = dc.vtweg
  AND so.SalesOrganization = dc.vkorg
LEFT JOIN b_lots.plcbe stc 
  ON so.SoldToParty = stc.kunnr
LEFT JOIN b_lots.plcbe shc 
  ON so.ShipToParty = shc.kunnr
LEFT JOIN b_lots.ploal pal 
  ON so.Plant = pal.werks
LEFT JOIN b_lots.plosl psl 
  ON so.StorageLocation = psl.lgort
  AND so.Plant = psl.werks
WHERE so.DATA_OPERATION <> 'D'
  AND year(cast(to_date(from_unixtime(unix_timestamp(cast(so.OrderCreationDate as string), 'yyyyMMdd'))) as date)) >= year(current_date()) - 3;