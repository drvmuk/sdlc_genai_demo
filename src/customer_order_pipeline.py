-- ============================================================================
-- Delta Live Tables Pipeline: Customer Order Processing
-- Implements medallion architecture with SCD Type 2 for historical tracking
-- ============================================================================

-- SECURITY RISK: EmailId contains PII data - ensure proper masking and access controls
-- SECURITY RISK: Customer Name is PII - implement row-level security and data governance
-- PERFORMANCE: Consider partitioning large tables by Date for better query performance
-- QUALITY: Schema enforcement applied at bronze layer for data validation

-- ============================================================================
-- BRONZE LAYER: Raw Data Ingestion
-- ============================================================================

-- Bronze Customer Table
-- Raw customer data loaded from CSV files
-- SECURITY: Contains PII (Name, EmailId) - restrict access appropriately
CREATE OR REFRESH STREAMING LIVE TABLE bronze_customer
COMMENT 'Raw customer data from CSV files'
AS SELECT 
    CustId,
    Name,
    EmailId,
    Region
FROM cloud_files(
    '/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata',
    'csv',
    map(
        'header', 'true',
        'schema', 'CustId STRING, Name STRING, EmailId STRING, Region STRING'
    )
);

-- Bronze Order Table
-- Raw order data loaded from CSV files
CREATE OR REFRESH STREAMING LIVE TABLE bronze_order
COMMENT 'Raw order data from CSV files'
AS SELECT 
    OrderId,
    ItemName,
    PricePerUnit,
    Qty,
    Date,
    CustId
FROM cloud_files(
    '/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata',
    'csv',
    map(
        'header', 'true',
        'schema', 'OrderId STRING, ItemName STRING, PricePerUnit DOUBLE, Qty INT, Date DATE, CustId STRING'
    )
);

-- ============================================================================
-- SILVER LAYER: Cleaned and Validated Data
-- ============================================================================

-- Silver Customer Table
-- Cleaned customer data with null filtering and deduplication
-- QUALITY: Removes records with any null values in key fields
-- QUALITY: Deduplicates based on CustId to ensure data integrity
CREATE OR REFRESH STREAMING LIVE TABLE silver_customer
COMMENT 'Cleaned customer data with nulls and duplicates removed'
AS SELECT 
    CustId,
    Name,
    EmailId,
    Region
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY CustId ORDER BY CustId) as rn
    FROM STREAM(LIVE.bronze_customer)
    WHERE CustId IS NOT NULL
        AND Name IS NOT NULL
        AND EmailId IS NOT NULL
        AND Region IS NOT NULL
)
WHERE rn = 1;

-- Silver Order Table
-- Cleaned order data with calculated TotalAmount column
-- QUALITY: Validates all required fields are non-null
-- PERFORMANCE: TotalAmount calculated once and stored for reuse
CREATE OR REFRESH STREAMING LIVE TABLE silver_order
COMMENT 'Cleaned order data with TotalAmount calculated'
AS SELECT 
    OrderId,
    ItemName,
    PricePerUnit,
    Qty,
    Date,
    CustId,
    PricePerUnit * Qty AS TotalAmount
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY OrderId ORDER BY OrderId) as rn
    FROM STREAM(LIVE.bronze_order)
    WHERE OrderId IS NOT NULL
        AND ItemName IS NOT NULL
        AND PricePerUnit IS NOT NULL
        AND Qty IS NOT NULL
        AND Date IS NOT NULL
        AND CustId IS NOT NULL
)
WHERE rn = 1;

-- ============================================================================
-- GOLD LAYER: Business Logic and Aggregations
-- ============================================================================

-- Order Summary Table with SCD Type 2
-- Tracks historical changes to customer and order data
-- GOVERNANCE: Change Data Feed enabled for audit trail
-- PERFORMANCE: Consider partitioning by Date for large datasets
-- SCD TYPE 2: IsActive flag indicates current records, StartDate and EndDate track validity period
CREATE OR REFRESH STREAMING LIVE TABLE ordersummary (
    CustId STRING,
    Name STRING,
    EmailId STRING,
    Region STRING,
    OrderId STRING,
    ItemName STRING,
    PricePerUnit DOUBLE,
    Qty INT,
    Date DATE,
    IsActive BOOLEAN,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP
)
TBLPROPERTIES (
    'quality' = 'silver',
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'SCD Type 2 table tracking customer and order data history';

-- Apply SCD Type 2 Logic using APPLY CHANGES
-- Automatically handles inserts, updates, and historical tracking
-- REFINEMENT: Consider adding sequence_by column for ordering changes
APPLY CHANGES INTO LIVE.ordersummary
FROM (
    SELECT 
        c.CustId,
        c.Name,
        c.EmailId,
        c.Region,
        o.OrderId,
        o.ItemName,
        o.PricePerUnit,
        o.Qty,
        o.Date,
        current_timestamp() AS processing_time
    FROM STREAM(LIVE.silver_customer) c
    INNER JOIN STREAM(LIVE.silver_order) o
        ON c.CustId = o.CustId
)
KEYS (CustId, OrderId)
SEQUENCE BY processing_time
STORED AS SCD TYPE 2;

-- Customer Aggregate Spend Table
-- Aggregates total spending by customer name and date
-- PERFORMANCE: Pre-aggregated for faster reporting queries
-- REFINEMENT: Consider adding Region dimension for regional analysis
CREATE OR REFRESH LIVE TABLE customeraggregatespend
COMMENT 'Aggregated customer spending by name and date'
AS SELECT 
    c.Name,
    o.Date,
    SUM(o.TotalAmount) AS TotalAmount
FROM LIVE.silver_order o
INNER JOIN LIVE.silver_customer c
    ON o.CustId = c.CustId
GROUP BY c.Name, o.Date;

-- ============================================================================
-- MIGRATION NOTES:
-- 1. Converted from DLT Python API to DLT SQL syntax
-- 2. Used APPLY CHANGES for SCD Type 2 instead of manual merge logic
-- 3. Replaced spark.read with cloud_files for streaming ingestion
-- 4. Used ROW_NUMBER for deduplication instead of dropDuplicates
-- 5. Streaming tables used for bronze and silver layers
-- 6. Materialized view used for gold layer aggregation
-- ============================================================================