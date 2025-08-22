%sql
-- Validation query for sales_orders_comp table
SELECT 
  COUNT(*) AS total_records,
  COUNT(DISTINCT CompOrderNumber) AS distinct_order_numbers,
  SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
  SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers,
  MIN(CompCreateDate) AS min_create_date,
  MAX(CompCreateDate) AS max_create_date,
  COUNT(DISTINCT CompSourceSystem) AS distinct_source_systems,
  COUNT(DISTINCT CompSiteId) AS distinct_site_ids
FROM sales_orders_comp;

-- Validate join conditions
SELECT 
  COUNT(*) AS total_records_in_comp,
  SUM(CASE WHEN e.whcode IS NOT NULL THEN 1 ELSE 0 END) AS records_with_site_id,
  SUM(CASE WHEN g.CustNo4 IS NOT NULL THEN 1 ELSE 0 END) AS records_with_ship_to_name,
  SUM(CASE WHEN h.CustNo4 IS NOT NULL THEN 1 ELSE 0 END) AS records_with_sold_to_name,
  SUM(CASE WHEN PLCAM.SCTCX IS NOT NULL OR PLPMS.SCPXXC IS NOT NULL THEN 1 ELSE 0 END) AS records_with_sales_dist_channel
FROM sales_orders_comp;

-- Validate date filtering
SELECT 
  YEAR(CompCreateDate) AS year,
  COUNT(*) AS record_count
FROM sales_orders_comp
GROUP BY YEAR(CompCreateDate)
ORDER BY YEAR(CompCreateDate);