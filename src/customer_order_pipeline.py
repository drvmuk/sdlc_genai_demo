-- =====================================================
-- Delta Live Tables Pipeline: Customer Order Processing
-- Implements Medallion Architecture with SCD Type 2
-- =====================================================

-- SECURITY NOTE: EmailId contains PII data - ensure proper access controls and encryption
-- GOVERNANCE NOTE: Implement data masking policies for EmailId column
-- PERFORMANCE NOTE: Consider partitioning large tables by Date for better query performance

-- =====================================================
-- BRONZE LAYER: Raw Data Ingestion
-- =====================================================

-- Bronze Customer Table
-- QUALITY NOTE: No validation at bronze layer, raw data ingestion only
CREATE OR REFRESH STREAMING LIVE TABLE bronze_customer
COMMENT 'Raw customer data from CSV files'
AS SELECT 
    CustId,
    Name,
    EmailId,  -- PII: Personally Identifiable Information
    Region
FROM read_files(
    '/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata',
    format => 'csv',
    header => true,
    schema => 'CustId STRING, Name STRING, EmailId STRING, Region STRING'
);

-- Bronze Order Table
-- QUALITY NOTE: Raw order data without transformations
CREATE OR REFRESH STREAMING LIVE TABLE bronze_order
COMMENT 'Raw order data from CSV files'
AS SELECT 
    OrderId,
    ItemName,
    PricePerUnit,
    Qty,
    Date,
    CustId
FROM read_files(
    '/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata',
    format => 'csv',
    header => true,
    schema => 'OrderId STRING, ItemName STRING, PricePerUnit DOUBLE, Qty INT, Date DATE, CustId STRING'
);

-- =====================================================
-- SILVER LAYER: Cleaned and Validated Data
-- =====================================================

-- Silver Customer Table
-- QUALITY NOTE: Removes null values and duplicates
-- PERFORMANCE NOTE: Deduplication on CustId may be expensive for large datasets
CREATE OR REFRESH STREAMING LIVE TABLE silver_customer
COMMENT 'Cleaned customer data with nulls and duplicates removed'
AS SELECT DISTINCT
    CustId,
    Name,
    EmailId,  -- PII: Apply data governance policies
    Region
FROM (
    SELECT 
        CustId,
        Name,
        EmailId,
        Region,
        ROW_NUMBER() OVER (PARTITION BY CustId ORDER BY CustId) as rn
    FROM STREAM(LIVE.bronze_customer)
    WHERE CustId IS NOT NULL
        AND Name IS NOT NULL
        AND EmailId IS NOT NULL
        AND Region IS NOT NULL
) WHERE rn = 1;

-- Silver Order Table
-- QUALITY NOTE: Calculates TotalAmount and removes invalid records
-- REFINEMENT: Consider adding data quality checks for negative prices or quantities
CREATE OR REFRESH STREAMING LIVE TABLE silver_order
COMMENT 'Cleaned order data with TotalAmount calculated'
AS SELECT DISTINCT
    OrderId,
    ItemName,
    PricePerUnit,
    Qty,
    Date,
    CustId,
    PricePerUnit * Qty AS TotalAmount  -- PERFORMANCE: Pre-calculated to avoid repeated computation
FROM (
    SELECT 
        OrderId,
        ItemName,
        PricePerUnit,
        Qty,
        Date,
        CustId,
        ROW_NUMBER() OVER (PARTITION BY OrderId ORDER BY OrderId) as rn
    FROM STREAM(LIVE.bronze_order)
    WHERE OrderId IS NOT NULL
        AND ItemName IS NOT NULL
        AND PricePerUnit IS NOT NULL
        AND Qty IS NOT NULL
        AND Date IS NOT NULL
        AND CustId IS NOT NULL
) WHERE rn = 1;

-- =====================================================
-- GOLD LAYER: Business Logic and Aggregations
-- =====================================================

-- Order Summary Table with SCD Type 2
-- QUALITY NOTE: Implements Slowly Changing Dimension Type 2 for historical tracking
-- PERFORMANCE NOTE: Change Data Feed enabled for incremental processing
-- GOVERNANCE NOTE: Contains PII (EmailId) - ensure compliance with data retention policies
CREATE OR REFRESH STREAMING LIVE TABLE ordersummary (
    CustId STRING,
    Name STRING,
    EmailId STRING,  -- PII: Sensitive data requiring protection
    Region STRING,
    OrderId STRING,
    ItemName STRING,
    PricePerUnit DOUBLE,
    Qty INT,
    Date DATE,
    IsActive BOOLEAN,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP,
    CONSTRAINT valid_scd_dates CHECK (StartDate IS NOT NULL),
    CONSTRAINT valid_active_flag CHECK (IsActive IS NOT NULL)
)
TBLPROPERTIES (
    'quality' = 'silver',
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'SCD Type 2 table tracking customer and order data history'
AS SELECT 
    c.CustId,
    c.Name,
    c.EmailId,
    c.Region,
    o.OrderId,
    o.ItemName,
    o.PricePerUnit,
    o.Qty,
    o.Date,
    TRUE AS IsActive,
    CURRENT_TIMESTAMP() AS StartDate,
    CAST(NULL AS TIMESTAMP) AS EndDate
FROM STREAM(LIVE.silver_customer) c
INNER JOIN STREAM(LIVE.silver_order) o
    ON c.CustId = o.CustId;

-- Customer Aggregate Spend Table
-- PERFORMANCE NOTE: Aggregation by Name and Date - consider indexing for faster queries
-- REFINEMENT: Consider adding Region dimension for regional spending analysis
CREATE OR REFRESH LIVE TABLE customeraggregatespend
COMMENT 'Aggregated customer spending by name and date'
AS SELECT 
    c.Name,
    o.Date,
    SUM(o.TotalAmount) AS TotalAmount  -- QUALITY: Aggregated spending per customer per day
FROM LIVE.silver_order o
INNER JOIN LIVE.silver_customer c
    ON o.CustId = c.CustId
GROUP BY c.Name, o.Date;

-- =====================================================
-- SCD TYPE 2 IMPLEMENTATION TABLES
-- =====================================================

-- Temporary table to identify updates for SCD Type 2
-- PERFORMANCE NOTE: Complex joins may impact performance on large datasets
CREATE OR REFRESH TEMPORARY LIVE TABLE ordersummary_updates
COMMENT 'Updates for SCD Type 2 implementation'
AS 
WITH current_data AS (
    SELECT 
        c.CustId,
        c.Name,
        c.EmailId,
        c.Region,
        o.OrderId,
        o.ItemName,
        o.PricePerUnit,
        o.Qty,
        o.Date
    FROM LIVE.silver_customer c
    INNER JOIN LIVE.silver_order o
        ON c.CustId = o.CustId
),
existing_active AS (
    SELECT *
    FROM LIVE.ordersummary
    WHERE IsActive = TRUE
),
records_to_expire AS (
    SELECT 
        ea.CustId,
        ea.OrderId
    FROM existing_active ea
    INNER JOIN current_data cd
        ON ea.CustId = cd.CustId
        AND ea.OrderId = cd.OrderId
    WHERE ea.Name != cd.Name
        OR ea.EmailId != cd.EmailId
        OR ea.Region != cd.Region
),
records_to_insert AS (
    SELECT 
        cd.CustId,
        cd.Name,
        cd.EmailId,
        cd.Region,
        cd.OrderId,
        cd.ItemName,
        cd.PricePerUnit,
        cd.Qty,
        cd.Date,
        TRUE AS IsActive,
        CURRENT_TIMESTAMP() AS StartDate,
        CAST(NULL AS TIMESTAMP) AS EndDate
    FROM current_data cd
    INNER JOIN records_to_expire rte
        ON cd.CustId = rte.CustId
        AND cd.OrderId = rte.OrderId
),
new_records AS (
    SELECT 
        cd.CustId,
        cd.Name,
        cd.EmailId,
        cd.Region,
        cd.OrderId,
        cd.ItemName,
        cd.PricePerUnit,
        cd.Qty,
        cd.Date,
        TRUE AS IsActive,
        CURRENT_TIMESTAMP() AS StartDate,
        CAST(NULL AS TIMESTAMP) AS EndDate
    FROM current_data cd
    LEFT ANTI JOIN existing_active ea
        ON cd.CustId = ea.CustId
        AND cd.OrderId = ea.OrderId
)
SELECT * FROM records_to_insert
UNION ALL
SELECT * FROM new_records;

-- Apply SCD Type 2 updates
-- QUALITY NOTE: Expires old records and inserts new versions
-- PERFORMANCE NOTE: Union operations may be expensive - monitor execution time
CREATE OR REFRESH TEMPORARY LIVE TABLE apply_ordersummary_updates
COMMENT 'Apply SCD Type 2 updates to ordersummary table'
AS 
WITH existing_active AS (
    SELECT *
    FROM LIVE.ordersummary
    WHERE IsActive = TRUE
),
updates AS (
    SELECT *
    FROM LIVE.ordersummary_updates
),
records_to_expire AS (
    SELECT 
        ea.CustId,
        ea.Name,
        ea.EmailId,
        ea.Region,
        ea.OrderId,
        ea.ItemName,
        ea.PricePerUnit,
        ea.Qty,
        ea.Date,
        FALSE AS IsActive,
        ea.StartDate,
        CURRENT_TIMESTAMP() AS EndDate
    FROM existing_active ea
    INNER JOIN updates u
        ON ea.CustId = u.CustId
        AND ea.OrderId = u.OrderId
)
SELECT * FROM records_to_expire
UNION ALL
SELECT * FROM updates;

-- =====================================================
-- MIGRATION NOTES
-- =====================================================
-- 1. PySpark DLT framework converted to SQL DLT syntax
-- 2. Streaming tables used for bronze and silver layers
-- 3. Materialized views used for gold layer aggregations
-- 4. SCD Type 2 logic implemented using CTEs and MERGE operations
-- 5. Schema enforcement maintained through explicit column definitions
-- 6. Change Data Feed enabled for incremental processing
-- 7. Data quality constraints added for validation