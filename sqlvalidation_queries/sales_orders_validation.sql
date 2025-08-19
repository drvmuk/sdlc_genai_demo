-- Validation query 1: Check if intermediate table was created successfully
%sql
SELECT COUNT(*) AS record_count
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation query 2: Check for null values in required fields in intermediate table
%sql
SELECT 
  SUM(CASE WHEN order_number IS NULL THEN 1 ELSE 0 END) AS null_order_number,
  SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date,
  SUM(CASE WHEN customer_number IS NULL THEN 1 ELSE 0 END) AS null_customer_number,
  SUM(CASE WHEN item_number IS NULL THEN 1 ELSE 0 END) AS null_item_number,
  SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity
FROM s_shared.sales_orders_hdr_itm_lots;

-- Validation query 3: Check if main table was created successfully
%sql
SELECT COUNT(*) AS record_count
FROM sales.sales_orders;

-- Validation query 4: Check for null values in required fields in main table
%sql
SELECT 
  SUM(CASE WHEN order_number IS NULL THEN 1 ELSE 0 END) AS null_order_number,
  SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date,
  SUM(CASE WHEN customer_number IS NULL THEN 1 ELSE 0 END) AS null_customer_number,
  SUM(CASE WHEN total_quantity IS NULL THEN 1 ELSE 0 END) AS null_total_quantity
FROM sales.sales_orders;

-- Validation query 5: Check data integrity between intermediate and main tables
%sql
WITH intermediate_summary AS (
  SELECT 
    order_number,
    COUNT(DISTINCT item_number) AS expected_total_items,
    SUM(quantity) AS expected_total_quantity,
    SUM(extended_price) AS expected_total_amount
  FROM s_shared.sales_orders_hdr_itm_lots
  GROUP BY order_number
)
SELECT 
  i.order_number,
  i.expected_total_items,
  m.total_items,
  i.expected_total_quantity,
  m.total_quantity,
  i.expected_total_amount,
  m.total_amount,
  CASE 
    WHEN i.expected_total_items = m.total_items 
     AND i.expected_total_quantity = m.total_quantity
     AND i.expected_total_amount = m.total_amount
    THEN 'MATCH'
    ELSE 'MISMATCH'
  END AS validation_status
FROM intermediate_summary i
JOIN sales.sales_orders m ON i.order_number = m.order_number
WHERE i.expected_total_items != m.total_items
   OR i.expected_total_quantity != m.total_quantity
   OR i.expected_total_amount != m.total_amount;

-- Validation query 6: Check distribution of order value categories
%sql
SELECT 
  order_value_category,
  COUNT(*) AS order_count,
  MIN(total_amount) AS min_amount,
  MAX(total_amount) AS max_amount,
  AVG(total_amount) AS avg_amount
FROM sales.sales_orders
GROUP BY order_value_category
ORDER BY order_value_category;

-- Validation query 7: Check for duplicate order numbers in main table
%sql
SELECT order_number, COUNT(*) AS duplicate_count
FROM sales.sales_orders
GROUP BY order_number
HAVING COUNT(*) > 1;