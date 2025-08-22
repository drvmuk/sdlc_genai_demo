%sql
-- Validation query for sales_orders_comp_stg table
SELECT 
  COUNT(*) AS total_records,
  COUNT(DISTINCT CompOrderNumber) AS distinct_order_numbers,
  SUM(CASE WHEN CompOrderNumber IS NULL THEN 1 ELSE 0 END) AS null_order_numbers,
  SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_numbers,
  MIN(CompCreateDate) AS min_create_date,
  MAX(CompCreateDate) AS max_create_date
FROM sales_orders_comp_stg;