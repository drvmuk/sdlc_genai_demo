%sql
-- Validation query to check if the mapping document table exists
SELECT COUNT(*) AS mapping_document_exists 
FROM information_schema.tables
WHERE table_name = 'mapping_document';

%sql
-- Validation query to check the structure of the mapping document table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'mapping_document'
ORDER BY ordinal_position;

%sql
-- Validation query to check if required columns exist in the mapping document
SELECT 
  SUM(CASE WHEN column_name = 'Column Name' THEN 1 ELSE 0 END) AS has_column_name,
  SUM(CASE WHEN column_name = 'Column Type' THEN 1 ELSE 0 END) AS has_column_type,
  SUM(CASE WHEN column_name = 'Source Table' THEN 1 ELSE 0 END) AS has_source_table,
  SUM(CASE WHEN column_name = 'Source Column/Transformation Logic' THEN 1 ELSE 0 END) AS has_source_column,
  SUM(CASE WHEN column_name = 'Join Condition' THEN 1 ELSE 0 END) AS has_join_condition
FROM information_schema.columns
WHERE table_name = 'mapping_document';

%sql
-- Validation query to check for missing values in required columns
SELECT 
  SUM(CASE WHEN `Column Name` IS NULL THEN 1 ELSE 0 END) AS null_column_name_count,
  SUM(CASE WHEN `Column Type` IS NULL THEN 1 ELSE 0 END) AS null_column_type_count,
  SUM(CASE WHEN `Source Table` IS NULL THEN 1 ELSE 0 END) AS null_source_table_count,
  SUM(CASE WHEN `Source Column/Transformation Logic` IS NULL THEN 1 ELSE 0 END) AS null_source_column_count
FROM mapping_document;

%sql
-- Validation query to check if all source tables exist
WITH distinct_source_tables AS (
  SELECT DISTINCT `Source Table` AS table_name
  FROM mapping_document
  WHERE `Source Table` IS NOT NULL
)
SELECT 
  t.table_name,
  CASE WHEN i.table_name IS NOT NULL THEN 'Exists' ELSE 'Missing' END AS status
FROM distinct_source_tables t
LEFT JOIN information_schema.tables i
  ON t.table_name = i.table_name;

%sql
-- Validation query to check for inconsistent join conditions
WITH join_tables AS (
  SELECT 
    `Source Table`,
    `Join Condition`,
    COUNT(*) AS condition_count
  FROM mapping_document
  WHERE `Join Condition` IS NOT NULL
  GROUP BY `Source Table`, `Join Condition`
)
SELECT 
  `Source Table`,
  `Join Condition`,
  condition_count
FROM join_tables
WHERE condition_count > 1
ORDER BY `Source Table`;