%sql
-- Step 1: Create Intermediate table with name sales_orders_comp_stg
CREATE OR REPLACE TABLE sales_orders_comp_stg AS
SELECT
    CONCAT(a.referenceto, a.refno) AS CompOrderNumber,
    a.stndard AS CompOrderType,
    b.bdcob AS CompSalesOrgCompanyCode,
    b.seqnumber AS CompLineNumber,
    CONCAT(a.custhead, a.custdtl) AS CompShipToNumber,
    CONCAT(a.cusnum, a.cusa) AS CompSoldToNumber,
    b.prodmatnum AS CompMaterialNumber,
    b.ordstline AS CompOrderStatusLineLast,
    a.ordsthdr AS CompOrderStatusHeader,
    a.osrc AS CompPoDocType,
    a.cref AS CompCustomerPo,
    CASE WHEN TRIM(a.refno) <> '' AND a.refnp <> 'NA' THEN CONCAT(a.reftp, a.refntp) ELSE '' END AS CompRelatedOrderNumber,
    CASE WHEN TRIM(a.reftpo) <> '' THEN a.reftpo ELSE '' END AS CompRelatedOrderType,
    b.ordq AS CompOrderQuantityOriginal,
    b.ordq AS CompOrderQuantityBase,
    b.tcqt AS CompCancelledQuantityOriginal,
    b.tdqt AS CompDeliveredQuantityOriginal,
    (b.ordq - b.tcqt - b.tdqt) AS CompOpenQuantityOrginal,
    (b.ordq - b.tcqt - b.tdqt) AS CompOpenQuantityBase,
    a.curchtyp AS CompCurrencyType,
    b.totalpric AS CompTotalPriceLocal,
    b.upbc AS CompUnitPriceLocal,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(a.dateupd AS STRING), 'yyyyMMdd'))) AS DATE) AS CompCreateDate,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(b.datecr AS STRING), 'yyyyMMdd'))) AS DATE) AS CompPromisedDeliveryDate,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(b.dtdel AS STRING), 'yyyyMMdd'))) AS DATE) AS CompRequestedDeliveryDate,
    CONCAT(a.cusn, a.cusa) AS CompPayerNumber,
    CONCAT_WS('-', b.exttx, CAST(b.seqne AS STRING)) AS CompOrderLineExtnCode,
    b.referenceno,
    b.reqno,
    b.exttax,
    b.seqnumber,
    a.enitycode
FROM 
    (SELECT * FROM b_source.ord_hdr WHERE delete_flag <> 'D') a
LEFT JOIN 
    b_source.ord_dtl b ON TRIM(a.referenceto) = TRIM(b.referenceto) 
    AND TRIM(a.refno) = TRIM(b.refno) 
    AND b.delete_flag <> 'D'