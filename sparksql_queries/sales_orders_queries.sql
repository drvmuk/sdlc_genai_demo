-- Step 1: Create Intermediate table s_shared.sales_orders_hdr_itm_lots
%sql
CREATE OR REPLACE TABLE s_shared.sales_orders_hdr_itm_lots AS
SELECT
    CONCAT(a.refto, a.refno) AS OrderNumber,
    a.stndx AS OrderType,
    b.bdcob AS SalesOrgCompanyCode,
    b.seqnl AS LineNumber,
    CONCAT(a.cusnh, a.cusah) AS ShipToNumber,
    CONCAT(a.cusn3, a.cusa3) AS SoldToNumber,
    b.prodx AS MaterialNumber,
    b.ordsl AS OrderStatusLineLast,
    a.ordsx AS OrderStatusHeader,
    a.osrcx AS PoDocType,
    a.crefx AS CustomerPo,
    CASE 
        WHEN TRIM(a.reftp) <> '' AND a.refnp <> 'NA' THEN CONCAT(a.reftp, a.refnp)
        ELSE ''
    END AS RelatedOrderNumber,
    CASE 
        WHEN TRIM(a.reftp) <> '' THEN a.reftp
        ELSE ''
    END AS RelatedOrderType,
    b.ordqx AS OrderQuantityOriginal,
    b.ordqx AS OrderQuantityBase,
    b.tcqtx AS CancelledQuantityOriginal,
    b.tdqtx AS DeliveredQuantityOriginal,
    (b.ordqx - b.tcqtx - b.tdqtx) AS OpenQuantityOrginal,
    (b.ordqx - b.tcqtx - b.tdqtx) AS OpenQuantityBase,
    a.curch AS CurrencyType,
    b.pricx AS TotalPriceLocal,
    b.upbcx AS UnitPriceLocal,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(a.datoxc AS STRING), 'yyyyMMdd'))) AS DATE) AS CreateDate,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(b.dreqlc AS STRING), 'yyyyMMdd'))) AS DATE) AS PromisedDeliveryDate,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(b.dreqo AS STRING), 'yyyyMMdd'))) AS DATE) AS RequestedDeliveryDate,
    CONCAT(a.cusn3, a.cusa3) AS PayerNumber,
    CONCAT_WS('-', b.exttx, CAST(b.seqne AS STRING)) AS OrderLineExtnCode,
    b.refto,
    b.dreqlc,
    b.exttx,
    b.seqne,
    a.entch
FROM
    (SELECT * FROM b_lots.plooh WHERE DATA_OPERATION <> 'D') a
LEFT JOIN
    b_lots.plool b
ON
    TRIM(a.refto) = TRIM(b.refto) 
    AND TRIM(a.refno) = TRIM(b.refno) 
    AND b.DATA_OPERATION <> 'D';

-- Step 2: Create Main table sales_orders
%sql
CREATE OR REPLACE TABLE s_shared.sales_orders AS
WITH site_data AS (
    SELECT DISTINCT 
        refto, refno, dreql, seqnl, exttx, seqne, whcda
    FROM 
        b_lots.ploal
    WHERE 
        DATA_OPERATION <> 'D' 
        AND TRIM(whcda) != '' 
        AND whcda IS NOT NULL
    
    UNION
    
    SELECT DISTINCT 
        refto, refno, dreql, seqnl, exttx, seqne, whcda
    FROM 
        b_lots.plosl
    WHERE 
        DATA_OPERATION <> 'D' 
        AND TRIM(whcda) != '' 
        AND whcda IS NOT NULL
    
    UNION
    
    SELECT DISTINCT 
        refth AS refto, 
        refnh AS refno, 
        drqhx AS dreql, 
        seqnh AS seqnl, 
        extth AS exttx, 
        seqni AS seqne, 
        whcdi AS whcda 
    FROM 
        b_lots.phihm
    WHERE 
        DATA_OPERATION <> 'D' 
        AND TRIM(whcdi) != '' 
        AND whcdi IS NOT NULL
),
plcam_data AS (
    SELECT 
        CUSNX, CUSAX, PRODX, MAX(SCTCX) AS SCTCX 
    FROM 
        b_lots.plcam 
    WHERE 
        DATA_OPERATION <> 'D' 
    GROUP BY 
        CUSNX, CUSAX, PRODX
),
phihm_data AS (
    SELECT 
        refth, refnh, seqnh, extth, seqni, MAX(datrx) AS datrx
    FROM 
        b_lots.phihm
    WHERE 
        DATA_OPERATION <> 'D'
    GROUP BY 
        refth, refnh, seqnh, extth, seqni
),
lecotw1_data AS (
    SELECT 
        MAX(dlshlc) AS dlshlc, refno, refto, dreql, seqnl
    FROM 
        b_lots.lecotw1
    GROUP BY 
        refno, refto, seqnl, dreql
),
latest_currency_conversion AS (
    SELECT 
        a.* 
    FROM 
        s_shared.currency_conversion a
    JOIN (
        SELECT MAX(from_date) latest_date 
        FROM s_shared.currency_conversion
    ) b
    ON TRIM(a.from_date) = TRIM(b.latest_date)
)

SELECT
    registry.StandardValue AS SourceSystem,
    a.OrderNumber,
    a.OrderType,
    a.SalesOrgCompanyCode,
    a.LineNumber,
    CASE
        WHEN TRIM(e.whcda) <> '' AND e.whcda IS NOT NULL THEN e.whcda
        ELSE b.whcdl
    END AS SiteId,
    a.ShipToNumber,
    a.SoldToNumber,
    a.MaterialNumber,
    NULL AS MaterialNumberHarmonized,
    a.OrderStatusLineLast,
    NULL AS OrderStatusLineNext,
    a.OrderStatusHeader,
    NULL AS OrderStatusLineHarmonized,
    NULL AS OrderStatusHeaderHarmonized,
    NULL AS ItemCategory,
    NULL AS ItemCategoryHarmonized,
    NULL AS ReasonForRejection,
    NULL AS ReasonForRejectionHarmonized,
    CASE
        WHEN TRIM(PLCAM.SCTCX) <> '' THEN PLCAM.SCTCX
        ELSE PLPMS.SCPXXC
    END AS SalesDistributionChannel,
    NULL AS SalesDistChannelHarmonized,
    NULL AS IntercoPlant,
    a.PoDocType,
    NULL AS PoDocTypeHarmonized,
    g.cusnx4 AS ShipToName,
    h.cusnx4 AS SoldToName,
    g.ctryc AS Country,
    g.adtbx AS City,
    NULL AS ProvinceState,
    g.adzbx AS PostalCode,
    g.adsbx AS Street1,
    NULL AS Street2,
    a.CustomerPo,
    a.RelatedOrderNumber,
    a.RelatedOrderType,
    NULL AS RelatedCompanyCode,
    NULL AS RelatedLineNumber,
    'EA' AS OrderUom,
    'EA' AS BaseUom,
    1 AS ConversionFactor,
    a.OrderQuantityOriginal,
    a.OrderQuantityBase,
    a.CancelledQuantityOriginal,
    NULL AS CancelledQuantityBase,
    a.DeliveredQuantityOriginal,
    a.OpenQuantityOrginal,
    a.OpenQuantityBase,
    NULL AS DeliveredQuantityBase,
    a.CurrencyType,
    CASE
        WHEN a.CurrencyType = 'USD' THEN 1
        ELSE cur.exchange_rate
    END AS ExchangeRate,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(cur.from_date AS STRING), 'yyyyMMdd'))) AS DATE) AS ExchangeDate,
    a.TotalPriceLocal,
    NULL AS TotalPriceUsd,
    a.UnitPriceLocal,
    NULL AS UnitPriceUsd,
    a.CreateDate,
    NULL AS SchedPickDate,
    NULL AS RevisedMaterialAvailablityDate,
    a.PromisedDeliveryDate,
    NULL AS ScheduledFirstDelDate,
    a.RequestedDeliveryDate,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(d.datrx AS STRING), 'yyyyMMdd'))) AS DATE) AS ActualShipDate,
    NULL AS LastChangeDate,
    NULL AS ExtractDate,
    NULL AS LineType,
    NULL AS ConfirmedQuantitySales,
    '' AS DefaultDeliveryBlock,
    '' AS DeliveryBlockHeader,
    '' AS CreditStatus,
    '' AS OverallStatus,
    NULL AS TotalPriceCondLocal,
    NULL AS TotalPriceCondLocalUsd,
    NULL AS Denominator,
    NULL AS Numerator,
    '' AS DocumentCategory,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(i.dlshlc AS STRING), 'yyyyMMdd'))) AS DATE) AS LatestProcessingDate,
    j.bo_scope AS BoScope,
    k.availability_descr AS AvailabilityDescription,
    l.bill_to_market AS BillToMarket,
    a.PayerNumber,
    m.cusnx4 AS PayerName,
    NULL AS OverallCompletionStatus,
    NULL AS SubTotal2,
    NULL AS HoldCode,
    NULL AS OriginalPromiseDeliveryDate,
    NULL AS BackOrderQuantityOriginal,
    NULL AS BackOrderQuantityBase,
    NULL AS ItemFlashMessage,
    NULL AS HeaderBusinessUnit,
    NULL AS TotalReplenishmentLeadTime,
    NULL AS PickPackTime,
    NULL AS RouteDays,
    NULL AS SystemStatus,
    NULL AS UserStatus,
    NULL AS WbsElement,
    NULL AS WbsElementHeader,
    NULL AS CustomerPromiseDate,
    NULL AS ShippingPoint,
    NULL AS Route,
    NULL AS ProfitCenter,
    NULL AS GoalDate,
    NULL AS ShipDueDate,
    a.OrderLineExtnCode,
    NULL AS SoItemOiginalCustomerPromisedDate,
    NULL AS SoItemRevisedCustomerPromisedDate,
    NULL AS MaterialDescriptionSales,
    NULL AS TotalPriceLocalCommops,
    NULL AS TotalPriceUsdCommops,
    NULL AS BatchNumber,
    NULL AS LocalCurrency,
    NULL AS NetValueDocCurrency,
    NULL AS PricingReferenceMaterial,
    NULL AS MaterialEntered,
    NULL AS NameOfTheOrderer,
    NULL AS OrderCreatedBy,
    NULL AS BillPlanNumber,
    NULL AS BillPlanType,
    NULL AS MaterialKeyReltio,
    NULL AS EnterpriseCustomerKey,
    NULL AS QuantityInBaseUnitOfMeasure,
    NULL AS GoodsMovementStatus,
    NULL AS MovementIndicator,
    NULL AS ShipToParty,
    NULL AS RevisedGoodsIssueDate,
    NULL AS CreatedOnLips,
    NULL AS MaterialGroup4,
    NULL AS ReferenceDocumentNumber,
    NULL AS ReferenceItemNumber,
    NULL AS MissingPoNumber,
    NULL AS FirstCommitDeliveryDate,
    NULL AS BillingBlock,
    NULL AS LatestCommitDeliveryDate,
    NULL AS OriginalCdd,
    NULL AS ContractStartDate,
    NULL AS ContractEndDate,
    NULL AS TargetQuantity,
    NULL AS Uom,
    NULL AS ContractNo,
    NULL AS ContractItem,
    NULL AS TargetValue,
    NULL AS AssortmentModule,
    NULL AS ContractNumberAtSo,
    NULL AS ConsumedQuantity,
    NULL AS StorageLocation,
    NULL AS Currency,
    NULL AS PoDate,
    NULL AS CreatedBy,
    NULL AS OrderReason,
    NULL AS SoHeaderLastChangeDate,
    NULL AS SoItemLastChangeDate,
    NULL AS OrderReasonHarmonized,
    NULL AS ShippingCondition,
    NULL AS OrderStatusHarmonized
FROM 
    s_shared.sales_orders_hdr_itm_lots a
LEFT JOIN 
    (SELECT * FROM b_lots.plooh WHERE DATA_OPERATION <> 'D') plooh
    ON TRIM(a.OrderNumber) = TRIM(CONCAT(plooh.refto, plooh.refno))
LEFT JOIN 
    b_lots.plool b
    ON TRIM(a.OrderNumber) = TRIM(CONCAT(b.refto, b.refno))
    AND a.LineNumber = b.seqnl
LEFT JOIN 
    site_data e
    ON TRIM(a.OrderNumber) = TRIM(CONCAT(e.refto, e.refno))
    AND a.dreqlc = e.dreql
    AND a.LineNumber = e.seqnl
    AND TRIM(a.exttx) = TRIM(e.exttx)
    AND a.seqne = e.seqne
LEFT JOIN 
    b_lots.plcbe g
    ON TRIM(a.ShipToNumber) = TRIM(CONCAT(g.cusnx, g.cusax))
    AND g.DATA_OPERATION <> 'D'
LEFT JOIN 
    (SELECT cusnx, cusax, cusnx4 FROM b_lots.plcbe WHERE DATA_OPERATION <> 'D') h
    ON TRIM(a.SoldToNumber) = TRIM(CONCAT(h.cusnx, h.cusax))
LEFT JOIN 
    (SELECT cusnx, cusax, cusnx4 FROM b_lots.plcbe WHERE DATA_OPERATION <> 'D') m
    ON TRIM(a.PayerNumber) = TRIM(CONCAT(m.cusnx, m.cusax))
LEFT JOIN 
    plcam_data PLCAM
    ON TRIM(CONCAT(PLCAM.CUSNX, PLCAM.CUSAX)) = TRIM(a.ShipToNumber)
    AND TRIM(PLCAM.PRODX) = TRIM(a.MaterialNumber)
LEFT JOIN 
    lots_staging.PLPMS_stg PLPMS
    ON TRIM(PLPMS.PRODX) = TRIM(a.MaterialNumber)
    AND PLPMS.DATA_OPERATION <> 'D'
LEFT JOIN 
    latest_currency_conversion cur
    ON TRIM(a.CurrencyType) = TRIM(cur.FromCurrency)
LEFT JOIN 
    phihm_data d
    ON TRIM(a.OrderNumber) = TRIM(CONCAT(d.refth, d.refnh))
    AND a.LineNumber = d.seqnh
    AND TRIM(a.exttx) = TRIM(d.extth)
    AND a.seqne = d.seqni
LEFT JOIN 
    lecotw1_data i
    ON TRIM(a.OrderNumber) = TRIM(CONCAT(i.refto, i.refno))
    AND a.dreqlc = i.dreql
    AND a.LineNumber = i.seqnl
LEFT JOIN 
    b_user_managed.rtbl_lots_ref_type_category_bo_scope j
    ON TRIM(plooh.refto) = TRIM(j.ref_type)
LEFT JOIN 
    (SELECT availability_descr, prodx FROM b_lots.lmpmt03) k
    ON TRIM(a.MaterialNumber) = TRIM(k.prodx)
LEFT JOIN 
    b_user_managed.rtbl_lots_market_region_cco l
    ON TRIM(a.entch) = TRIM(l.entch)
LEFT JOIN 
    (SELECT StandardValue FROM s_shared.isc_registry 
     WHERE TRIM(Canonical) = 'ALL' 
     AND TRIM(RegistryCode) = 'SOURCE_SYSTEM_CODE' 
     AND TRIM(sourceSystem) = 'GLOBAL' 
     AND TRIM(sourceSystemValue) = 'LOTS') registry
    ON 1=1
WHERE 
    a.DATA_OPERATION <> 'D'
    AND YEAR(a.CreateDate) >= YEAR(CURRENT_DATE()) - 3;