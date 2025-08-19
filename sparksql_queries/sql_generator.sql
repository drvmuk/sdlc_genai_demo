%sql
-- SQL Generator Function
-- This function takes a mapping document table and generates a SQL query

CREATE OR REPLACE FUNCTION generate_sql_from_mapping(mapping_table_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  WITH source_tables AS (
    -- Extract distinct source tables with their join conditions
    SELECT DISTINCT 
      `Source Table` AS table_name,
      FIRST(`Join Condition`) AS join_condition
    FROM ${mapping_table_name}
    WHERE `Source Table` IS NOT NULL
    GROUP BY `Source Table`
  ),
  
  base_table AS (
    -- Identify the base table (the one without join condition)
    SELECT table_name
    FROM source_tables
    WHERE join_condition IS NULL OR TRIM(join_condition) = ''
    LIMIT 1
  ),
  
  select_columns AS (
    -- Build the SELECT clause
    SELECT 
      CONCAT(
        'SELECT ',
        STRING_AGG(
          CASE 
            -- If transformation logic exists, use it
            WHEN `Source Column/Transformation Logic` LIKE '%(%' OR 
                 `Source Column/Transformation Logic` LIKE '%||%' OR
                 `Source Column/Transformation Logic` LIKE '%CASE%' OR
                 `Source Column/Transformation Logic` LIKE '%WHEN%'
              THEN CONCAT(`Source Column/Transformation Logic`, ' AS `', `Column Name`, '`')
            -- Otherwise use direct column reference
            ELSE CONCAT(
              CASE WHEN INSTR(`Source Column/Transformation Logic`, '.') > 0 
                THEN `Source Column/Transformation Logic` 
                ELSE CONCAT(
                  SUBSTRING_INDEX(`Source Table`, ' ', -1), 
                  '.', 
                  `Source Column/Transformation Logic`
                ) 
              END,
              ' AS `', `Column Name`, '`'
            )
          END,
          ',\n  '
        )
      ) AS select_clause
    FROM ${mapping_table_name}
    WHERE `Column Name` IS NOT NULL
  ),
  
  from_clause AS (
    -- Build the FROM clause
    SELECT 
      CONCAT(
        'FROM ',
        (SELECT table_name FROM base_table),
        ' AS ',
        SUBSTRING_INDEX((SELECT table_name FROM base_table), ' ', -1)
      ) AS from_clause
  ),
  
  join_clauses AS (
    -- Build the JOIN clauses
    SELECT 
      STRING_AGG(
        CONCAT(
          'LEFT JOIN ',
          st.table_name,
          ' AS ',
          SUBSTRING_INDEX(st.table_name, ' ', -1),
          ' ON ',
          st.join_condition
        ),
        '\n'
      ) AS join_clause
    FROM source_tables st
    WHERE st.join_condition IS NOT NULL
    AND st.table_name != (SELECT table_name FROM base_table)
  )
  
  -- Combine all parts to form the complete SQL query
  SELECT CONCAT(
    sc.select_clause,
    '\n',
    fc.from_clause,
    '\n',
    COALESCE(jc.join_clause, ''),
    ';'
  ) AS generated_sql
  FROM select_columns sc
  CROSS JOIN from_clause fc
  CROSS JOIN join_clauses jc
$$;

%sql
-- Example usage of the SQL generator function
-- Assuming 'mapping_document' is the table containing the mapping information
SELECT generate_sql_from_mapping('mapping_document') AS generated_sql;

%sql
-- Create a view that will generate SQL from the mapping document
CREATE OR REPLACE VIEW generated_sql_view AS
SELECT generate_sql_from_mapping('mapping_document') AS sql_statement;

%sql
-- SQL to implement the SQL generation service as a stored procedure
CREATE OR REPLACE PROCEDURE generate_sql_statement(mapping_table_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
  -- Validate mapping document structure
  DECLARE mapping_exists INT;
  DECLARE required_columns_count INT;
  DECLARE error_message STRING DEFAULT NULL;
  
  -- Check if mapping table exists
  SELECT COUNT(*) INTO mapping_exists
  FROM information_schema.tables
  WHERE table_name = mapping_table_name;
  
  IF mapping_exists = 0 THEN
    RETURN CONCAT('ERROR: Mapping table "', mapping_table_name, '" does not exist.');
  END IF;
  
  -- Check if required columns exist
  SELECT 
    SUM(
      CASE 
        WHEN column_name IN ('Column Name', 'Column Type', 'Source Table', 
                           'Source Column/Transformation Logic', 'Join Condition') 
        THEN 1 
        ELSE 0 
      END
    ) INTO required_columns_count
  FROM information_schema.columns
  WHERE table_name = mapping_table_name;
  
  IF required_columns_count < 5 THEN
    RETURN 'ERROR: Mapping table is missing one or more required columns.';
  END IF;
  
  -- Generate the SQL statement
  RETURN (SELECT generate_sql_from_mapping(mapping_table_name));
END;

%sql
-- Example of calling the stored procedure
CALL generate_sql_statement('mapping_document');

%sql
-- Create a logging table for SQL generation operations
CREATE TABLE IF NOT EXISTS sql_generation_log (
  log_id BIGINT GENERATED ALWAYS AS IDENTITY,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  mapping_table STRING,
  status STRING,
  message STRING,
  generated_sql STRING
);

%sql
-- Create a procedure that generates SQL and logs the operation
CREATE OR REPLACE PROCEDURE generate_and_log_sql(mapping_table_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
  DECLARE result STRING;
  DECLARE status STRING DEFAULT 'SUCCESS';
  DECLARE message STRING DEFAULT NULL;
  
  -- Try to generate SQL
  BEGIN
    SET result = (CALL generate_sql_statement(mapping_table_name));
    
    -- Check if result starts with ERROR
    IF SUBSTRING(result, 1, 5) = 'ERROR' THEN
      SET status = 'ERROR';
      SET message = result;
      SET result = NULL;
    END IF;
    
    EXCEPTION WHEN OTHER THEN
      SET status = 'ERROR';
      SET message = SQLERRM;
      SET result = NULL;
  END;
  
  -- Log the operation
  INSERT INTO sql_generation_log (mapping_table, status, message, generated_sql)
  VALUES (mapping_table_name, status, message, result);
  
  -- Return the result
  RETURN result;
END;

%sql
-- Example of calling the logging procedure
CALL generate_and_log_sql('mapping_document');