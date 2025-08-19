-- Generate SQL for intermediate table "s_shared.sales_orders_hdr_itm_lots"
%sql
CREATE OR REPLACE TABLE s_shared.sales_orders_hdr_itm_lots AS
WITH input_mapping AS (
  SELECT 
    source_table,
    source_column,
    target_column,
    data_type,
    transformation_logic,
    is_required
  FROM s_shared.input_mapping_table
  WHERE target_table = 's_shared.sales_orders_hdr_itm_lots'
),
header_columns AS (
  SELECT
    CONCAT('CAST(', 
           CASE 
             WHEN transformation_logic IS NOT NULL AND transformation_logic != '' 
             THEN transformation_logic 
             ELSE source_column 
           END, 
           ' AS ', data_type, ') AS ', target_column) AS column_expr,
    ROW_NUMBER() OVER (ORDER BY target_column) AS rn
  FROM input_mapping
  WHERE source_table = 'b_lots.plooh'
),
item_columns AS (
  SELECT
    CONCAT('CAST(', 
           CASE 
             WHEN transformation_logic IS NOT NULL AND transformation_logic != '' 
             THEN transformation_logic 
             ELSE source_column 
           END, 
           ' AS ', data_type, ') AS ', target_column) AS column_expr,
    ROW_NUMBER() OVER (ORDER BY target_column) AS rn
  FROM input_mapping
  WHERE source_table = 'b_lots.plool'
)
SELECT
  -- Dynamically construct columns from header and item tables based on mapping
  h.ORDNBR AS order_number,
  h.ORDDAT AS order_date,
  h.CUSTNBR AS customer_number,
  h.CUSTNME AS customer_name,
  h.ORDRTYP AS order_type,
  h.ORDSTS AS order_status,
  i.ITEMNBR AS item_number,
  i.ITEMDSC AS item_description,
  i.QTY AS quantity,
  i.UNITPRC AS unit_price,
  i.EXTPRC AS extended_price,
  i.LOTNBR AS lot_number,
  i.LOTQTY AS lot_quantity,
  -- Add audit columns
  CURRENT_TIMESTAMP() AS created_timestamp,
  'SYSTEM' AS created_by
FROM b_lots.plooh h
JOIN b_lots.plool i ON h.ORDNBR = i.ORDNBR
WHERE h.ORDSTS != 'CANCELLED'
  AND i.QTY > 0;

-- Generate SQL for main table "sales_orders"
%sql
CREATE OR REPLACE TABLE sales.sales_orders AS
WITH input_mapping AS (
  SELECT 
    source_table,
    source_column,
    target_column,
    data_type,
    transformation_logic,
    is_required
  FROM s_shared.input_mapping_table
  WHERE target_table = 'sales.sales_orders'
)
SELECT
  order_number,
  order_date,
  customer_number,
  customer_name,
  order_type,
  order_status,
  COUNT(DISTINCT item_number) AS total_items,
  SUM(quantity) AS total_quantity,
  SUM(extended_price) AS total_amount,
  MIN(lot_number) AS first_lot_number,
  -- Add derived columns
  CASE 
    WHEN SUM(extended_price) > 10000 THEN 'High Value'
    WHEN SUM(extended_price) > 5000 THEN 'Medium Value'
    ELSE 'Low Value'
  END AS order_value_category,
  -- Add audit columns
  CURRENT_TIMESTAMP() AS created_timestamp,
  'SYSTEM' AS created_by,
  CURRENT_TIMESTAMP() AS modified_timestamp,
  'SYSTEM' AS modified_by
FROM s_shared.sales_orders_hdr_itm_lots
GROUP BY 
  order_number,
  order_date,
  customer_number,
  customer_name,
  order_type,
  order_status;