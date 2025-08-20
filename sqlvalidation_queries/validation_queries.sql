-- Validation query for s_shared.sales_orders_hdr_itm_lots
%sql
SELECT 
  COUNT(*) AS total_records,
  COUNT(DISTINCT OrderNumber) AS distinct_orders,
  COUNT(DISTINCT MaterialNumber) AS distinct_materials,
  MIN(CreateDate) AS min_create_date,
  MAX(CreateDate) AS max_create_date
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation query for s_shared.sales_orders
%sql
SELECT 
  COUNT(*) AS total_records,
  COUNT(DISTINCT OrderNumber) AS distinct_orders,
  COUNT(DISTINCT MaterialNumber) AS distinct_materials,
  MIN(CreateDate) AS min_create_date,
  MAX(CreateDate) AS max_create_date,
  COUNT(CASE WHEN SiteId IS NOT NULL THEN 1 END) AS records_with_site_id,
  COUNT(CASE WHEN ShipToName IS NOT NULL THEN 1 END) AS records_with_ship_to_name,
  COUNT(CASE WHEN SoldToName IS NOT NULL THEN 1 END) AS records_with_sold_to_name,
  COUNT(CASE WHEN ActualShipDate IS NOT NULL THEN 1 END) AS records_with_actual_ship_date
FROM s_shared.sales_orders;

-- Validation query to check join between s_shared.sales_orders_hdr_itm_lots and site data
%sql
SELECT 
  COUNT(*) AS total_records,
  COUNT(DISTINCT b.OrderNumber) AS distinct_orders,
  COUNT(CASE WHEN e.whcda IS NOT NULL THEN 1 END) AS records_with_site_from_join
FROM s_shared.sales_orders_hdr_itm_lots b
LEFT JOIN
  (SELECT 
     refto, refno, dreql, seqnl, exttx, seqne, whcda
   FROM b_lots.ploal
   WHERE DATA_OPERATION <> 'D'
     AND TRIM(whcda) != ''
     AND whcda IS NOT NULL
   
   UNION
   
   SELECT 
     refto, refno, dreql, seqnl, exttx, seqne, whcda
   FROM b_lots.plosl
   WHERE DATA_OPERATION <> 'D'
     AND TRIM(whcda) != ''
     AND whcda IS NOT NULL
   
   UNION
   
   SELECT 
     refth AS refto,
     refnh AS refno,
     drqhx AS dreql,
     seqnh AS seqnl,
     extth AS exttx,
     seqni AS seqne,
     whcdi AS whcda
   FROM b_lots.phihm
   WHERE DATA_OPERATION <> 'D'
     AND TRIM(whcdi) != ''
     AND whcdi IS NOT NULL) e
ON
  TRIM(b.OrderNumber) = TRIM(CONCAT(e.refto, e.refno))
  AND b.dreqlc = e.dreql
  AND b.LineNumber = e.seqnl
  AND TRIM(b.exttx) = TRIM(e.exttx)
  AND b.seqne = e.seqne;

-- Validation query to check currency conversion
%sql
SELECT 
  a.CurrencyType,
  COUNT(*) AS record_count,
  COUNT(CASE WHEN cur.exchange_rate IS NOT NULL THEN 1 END) AS records_with_exchange_rate,
  MIN(cur.exchange_rate) AS min_exchange_rate,
  MAX(cur.exchange_rate) AS max_exchange_rate
FROM s_shared.sales_orders_hdr_itm_lots a
LEFT JOIN
  (SELECT
     a.*
   FROM s_shared.currency_conversion a
   JOIN
     (SELECT MAX(from_date) latest_date
      FROM s_shared.currency_conversion) b
   ON TRIM(a.from_date) = TRIM(b.latest_date)) cur
ON
  TRIM(a.CurrencyType) = TRIM(cur.FromCurrency)
GROUP BY a.CurrencyType;