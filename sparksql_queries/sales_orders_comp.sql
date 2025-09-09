%sql
-- Step 2: Create Main table "sales_orders_comp"
CREATE OR REPLACE TABLE sales_orders_comp AS
SELECT
    reg.StdValue AS CompSourceSystem,
    a.OrderNumber AS CompOrderNumber,
    a.OrderType AS CompOrderType,
    a.SalesOrgCompanyCode AS CompSalesOrgCompanyCode,
    a.LineNumber AS CompLineNumber,
    CASE 
        WHEN TRIM(e.whcode) <> '' AND e.whcode IS NOT NULL THEN e.whcode
        ELSE b.whcdl
    END AS CompSiteId,
    a.ShipToNumber AS CompShipToNumber,
    a.SoldToNumber AS CompSoldToNumber,
    a.MaterialNumber AS CompMaterialNumber,
    NULL AS CompMaterialNumberHarmonized,
    a.OrderStatusLineLast AS CompOrderStatusLineLast,
    NULL AS CompOrderStatusLineNext,
    a.OrderStatusHeader AS CompOrderStatusHeader,
    NULL AS CompOrderStatusLineHarmonized,
    NULL AS CompOrderStatusHeaderHarmonized,
    NULL AS CompItemCategory,
    NULL AS CompItemCategoryHarmonized,
    NULL AS CompReasonForRejection,
    NULL AS CompReasonForRejectionHarmonized,
    CASE 
        WHEN TRIM(PLCAM.SCTCX) <> '' THEN PLCAM.SCTCX
        ELSE PLPMS.SCPXXC
    END AS CompSalesDistributionChannel,
    NULL AS CompSalesDistChannelHarmonized,
    NULL AS CompIntercoPlant,
    a.PoDocType AS CompPoDocType,
    NULL AS CompPoDocTypeHarmonized,
    g.CustNo4 AS CompShipToName,
    h.CustNo4 AS CompSoldToName,
    g.ctryc AS CompCountry,
    g.adtbx AS CompCity,
    NULL AS CompProvinceState,
    g.adzbx AS CompPostalCode,
    g.adsbx AS CompStreet1,
    NULL AS CompStreet2,
    a.CustomerPo AS CompCustomerPo,
    a.RelatedOrderNumber AS CompRelatedOrderNumber,
    a.RelatedOrderType AS CompRelatedOrderType,
    NULL AS CompRelatedCompanyCode,
    NULL AS CompRelatedLineNumber,
    'EA' AS CompOrderUom,
    'EA' AS CompBaseUom,
    1 AS CompConversionFactor,
    a.OrderQuantityOriginal AS CompOrderQuantityOriginal,
    a.OrderQuantityBase AS CompOrderQuantityBase,
    a.CancelledQuantityOriginal AS CompCancelledQuantityOriginal,
    NULL AS CompCancelledQuantityBase,
    a.DeliveredQuantityOriginal AS CompDeliveredQuantityOriginal,
    a.OpenQuantityOrginal AS CompOpenQuantityOrginal,
    a.OpenQuantityBase AS CompOpenQuantityBase,
    NULL AS CompDeliveredQuantityBase,
    a.CurrencyType AS CompCurrencyType,
    CASE 
        WHEN a.curch = 'USD' THEN 1
        ELSE cur.exchange_rate
    END AS CompExchangeRate,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(cur.from_date AS STRING), 'yyyyMMdd'))) AS DATE) AS CompExchangeDate,
    a.TotalPriceLocal AS CompTotalPriceLocal,
    NULL AS CompTotalPriceUsd,
    a.UnitPriceLocal AS CompUnitPriceLocal,
    NULL AS CompUnitPriceUsd,
    a.CreateDate AS CompCreateDate,
    NULL AS CompSchedPickDate,
    NULL AS CompRevisedMaterialAvailablityDate,
    a.PromisedDeliveryDate AS CompPromisedDeliveryDate,
    NULL AS CompScheduledFirstDelDate,
    a.RequestedDeliveryDate AS CompRequestedDeliveryDate,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(d.datrx AS STRING), 'yyyyMMdd'))) AS DATE) AS CompActualShipDate,
    NULL AS CompLastChangeDate,
    NULL AS CompExtractDate,
    NULL AS CompLineType,
    NULL AS CompConfirmedQuantitySales,
    '' AS CompDefaultDeliveryBlock,
    '' AS CompDeliveryBlockHeader,
    '' AS CompCreditStatus,
    '' AS CompOverallStatus,
    NULL AS CompTotalPriceCondLocal,
    NULL AS CompTotalPriceCondLocalUsd,
    NULL AS CompDenominator,
    NULL AS CompNumerator,
    '' AS CompDocumentCategory,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(i.dlshlc AS STRING), 'yyyyMMdd'))) AS DATE) AS CompLatestProcessingDate,
    j.bo_scope AS CompBoScope,
    k.availability_descr AS CompAvailabilityDescription,
    l.bill_to_market AS CompBillToMarket,
    a.PayerNumber AS CompPayerNumber,
    m.CustNo4 AS CompPayerName,
    NULL AS CompOverallCompletionStatus,
    NULL AS CompSubTotal2,
    NULL AS CompHoldCode,
    NULL AS CompOriginalPromiseDeliveryDate,
    NULL AS CompBackOrderQuantityOriginal,
    NULL AS CompBackOrderQuantityBase,
    NULL AS CompItemFlashMessage,
    NULL AS CompHeaderBusinessUnit,
    NULL AS CompTotalReplenishmentLeadTime,
    NULL AS CompPickPackTime,
    NULL AS CompRouteDays,
    NULL AS CompSystemStatus,
    NULL AS CompUserStatus,
    NULL AS CompWbsElement,
    NULL AS CompWbsElementHeader,
    NULL AS CompCustomerPromiseDate,
    NULL AS CompShippingPoint,
    NULL AS CompRoute,
    NULL AS CompProfitCenter,
    NULL AS CompGoalDate,
    NULL AS CompShipDueDate,
    a.OrderLineExtnCode AS CompOrderLineExtnCode,
    NULL AS CompSoItemOiginalCustomerPromisedDate,
    NULL AS CompSoItemRevisedCustomerPromisedDate,
    NULL AS CompMaterialDescriptionSales,
    NULL AS CompTotalPriceLocalCommops,
    NULL AS CompTotalPriceUsdCommops,
    NULL AS CompBatchNumber,
    NULL AS CompLocalCurrency,
    NULL AS CompNetValueDocCurrency,
    NULL AS CompPricingReferenceMaterial,
    NULL AS CompMaterialEntered,
    NULL AS CompNameOfTheOrderer,
    NULL AS CompOrderCreatedBy,
    NULL AS CompBillPlanNumber,
    NULL AS CompBillPlanType,
    NULL AS CompMaterialKeyReltio,
    NULL AS CompEnterpriseCustomerKey,
    NULL AS CompQuantityInBaseUnitOfMeasure,
    NULL AS CompGoodsMovementStatus,
    NULL AS CompMovementIndicator,
    NULL AS CompShipToParty,
    NULL AS CompRevisedGoodsIssueDate,
    NULL AS CompCreatedOnLips,
    NULL AS CompMaterialGroup4,
    NULL AS CompReferenceDocumentNumber,
    NULL AS CompReferenceItemNumber,
    NULL AS CompMissingPoNumber,
    NULL AS CompFirstCommitDeliveryDate,
    NULL AS CompBillingBlock,
    NULL AS CompLatestCommitDeliveryDate,
    NULL AS CompOriginalCdd,
    NULL AS CompContractStartDate,
    NULL AS CompContractEndDate,
    NULL AS CompTargetQuantity,
    NULL AS CompUom,
    NULL AS CompContractNo,
    NULL AS CompContractItem,
    NULL AS CompTargetValue,
    NULL AS CompAssortmentModule,
    NULL AS CompContractNumberAtSo,
    NULL AS CompConsumedQuantity,
    NULL AS CompStorageLocation,
    NULL AS CompCurrency,
    NULL AS CompPoDate,
    NULL AS CompCreatedBy,
    NULL AS CompOrderReason,
    NULL AS CompSoHeaderLastChangeDate,
    NULL AS CompSoItemLastChangeDate,
    NULL AS CompOrderReasonHarmonized,
    NULL AS CompShippingCondition,
    NULL AS CompOrderStatusHarmonized
FROM 
    s_master.sale_orders a
LEFT JOIN 
    s_master.sale_orders b ON a.OrderNumber = b.OrderNumber AND a.LineNumber = b.LineNumber
LEFT JOIN 
    (SELECT StdValue 
     FROM s_master.ord_reg 
     WHERE TRIM(Type) = 'ALL' 
     AND TRIM(RegCode) = 'SOURCE_SYSTEM_CODE' 
     AND TRIM(sourceSystem) = 'GLOBAL' 
     AND TRIM(sourceSystemValue) = 'SAP') reg
LEFT JOIN 
    (SELECT DISTINCT 
        referenceto, referencefrom, requestno, seqnum, exttax, seqline, whcode 
     FROM b_source.ord_site 
     WHERE delete_flag <> 'D' AND TRIM(whcode) != '' AND whcode IS NOT NULL
     
     UNION
     
     SELECT DISTINCT 
        referenceto, referencefrom, requestno, seqnum, exttax, seqline, whcode 
     FROM b_source.ord_addr 
     WHERE delete_flag <> 'D' AND TRIM(whcode) != '' AND whcode IS NOT NULL
     
     UNION
     
     SELECT DISTINCT 
        refth AS referenceto, 
        refnh AS referencefrom, 
        drqhx AS requestno, 
        seqnh AS seqnum, 
        extth AS exttax, 
        seqni AS seqline, 
        whcdi AS whcode 
     FROM b_source.ord_reg 
     WHERE delete_flag <> 'D' AND TRIM(whcdi) != '' AND whcdi IS NOT NULL) e
    ON TRIM(b.OrderNumber) = TRIM(CONCAT(e.referenceto, e.referencefrom))
    AND b.requestnoc = e.requestno
    AND b.LineNumber = e.seqnum
    AND TRIM(b.exttax) = TRIM(e.exttax)
    AND b.seqline = e.seqline
LEFT JOIN 
    b_source.ord_addr g
    ON TRIM(a.ShipToNumber) = TRIM(CONCAT(g.CustNo, g.CustName)) AND g.delete_flag <> 'D'
LEFT JOIN 
    (SELECT CustNo, CustName, CustNo4 
     FROM b_source.ord_addr 
     WHERE delete_flag <> 'D') h
    ON TRIM(a.SoldToNumber) = TRIM(CONCAT(h.CustNo, h.CustName))
LEFT JOIN 
    (SELECT CustNo, CustName, CustNo4 
     FROM b_source.ord_addr 
     WHERE delete_flag <> 'D') m
    ON TRIM(a.PayerNumber) = TRIM(CONCAT(m.CustNo, m.CustName))
LEFT JOIN 
    (SELECT CustNo, CustName, PRODX, MAX(SCTCX) AS SCTCX 
     FROM b_source.ord_channel 
     WHERE delete_flag <> 'D' 
     GROUP BY CustNo, CustName, PRODX) PLCAM
    ON TRIM(CONCAT(PLCAM.CustNo, PLCAM.CustName)) = TRIM(a.ShipToNumber)
    AND TRIM(PLCAM.PRODX) = TRIM(b.MaterialNumber)
LEFT JOIN 
    b_source.ord_stat PLPMS
    ON TRIM(PLPMS.PRODX) = TRIM(b.MaterialNumber) AND PLPMS.delete_flag <> 'D'
LEFT JOIN 
    (SELECT a.* 
     FROM s_master.currency a
     JOIN (SELECT MAX(from_date) latest_date FROM s_master.currency) b
     ON TRIM(a.from_date) = TRIM(b.latest_date)) cur
    ON TRIM(a.CurrencyType) = TRIM(cur.FromCurrency)
LEFT JOIN 
    (SELECT refth, refnh, seqnh, extth, seqni, MAX(datrx) AS datrx
     FROM b_source.ord_reg
     WHERE delete_flag <> 'D'
     GROUP BY refth, refnh, seqnh, extth, seqni) d
    ON TRIM(b.OrderNumber) = TRIM(CONCAT(d.refth, d.refnh))
    AND b.LineNumber = d.seqnh
    AND TRIM(b.exttax) = TRIM(d.extth)
    AND b.seqline = d.seqni
LEFT JOIN 
    (SELECT MAX(dlshlc) AS dlshlc, referencefrom, referenceto, requestno, seqnum
     FROM b_source.ord_type 
     GROUP BY referencefrom, referenceto, seqnum, requestno) i
    ON TRIM(a.OrderNumber) = TRIM(CONCAT(i.referencefrom, i.referenceto))
    AND a.requestnoc = i.requestno
    AND a.LineNumber = i.seqnum
LEFT JOIN 
    (SELECT * FROM b_source.Boscope WHERE delete_flag <> 'D') j
    ON TRIM(a.referenceto) = TRIM(j.ref_type)
LEFT JOIN 
    (SELECT availability_descr, prodx FROM b_source.ord_cat) k
    ON TRIM(b.MaterialNumber) = TRIM(k.prodx)
LEFT JOIN 
    b_source.ord_reg l
    ON TRIM(a.entcho) = TRIM(l.entchi)
WHERE 
    a.delete_flag <> 'D'
    AND YEAR(CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(a.processed_date AS STRING), 'yyyyMMdd'))) AS DATE)) >= YEAR(CURRENT_DATE()) - 3