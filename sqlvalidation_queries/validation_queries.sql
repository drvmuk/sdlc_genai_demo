%sql
-- Validation query for sales_orders_comp_stg
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT CompOrderNumber) AS distinct_orders,
    SUM(CASE WHEN CompMaterialNumber IS NULL THEN 1 ELSE 0 END) AS null_material_count,
    MIN(CompCreateDate) AS min_create_date,
    MAX(CompCreateDate) AS max_create_date
FROM 
    sales_orders_comp_stg;

-- Validate join between ord_hdr and ord_dtl
%sql
SELECT 
    COUNT(*) AS total_join_records
FROM 
    (SELECT * FROM b_source.ord_hdr WHERE delete_flag <> 'D') a
LEFT JOIN 
    b_source.ord_dtl b 
ON 
    TRIM(a.referenceto) = TRIM(b.referenceto) 
    AND TRIM(a.refno) = TRIM(b.refno) 
    AND b.delete_flag <> 'D';

-- Validation query for sales_orders_comp
%sql
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT CompOrderNumber) AS distinct_orders,
    COUNT(DISTINCT CompSiteId) AS distinct_sites,
    SUM(CASE WHEN CompShipToName IS NULL THEN 1 ELSE 0 END) AS null_shipto_name,
    MIN(CompCreateDate) AS min_create_date,
    MAX(CompCreateDate) AS max_create_date
FROM 
    sales_orders_comp;

-- Validate the date filtering condition
%sql
SELECT 
    YEAR(CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(processed_date AS STRING), 'yyyyMMdd'))) AS DATE)) AS process_year,
    COUNT(*) AS record_count
FROM 
    s_master.sale_orders
WHERE 
    delete_flag <> 'D'
GROUP BY 
    YEAR(CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(processed_date AS STRING), 'yyyyMMdd'))) AS DATE))
ORDER BY 
    process_year;

-- Validate the complex site ID logic
%sql
WITH site_data AS (
    SELECT 
        b.OrderNumber,
        b.LineNumber,
        e.whcode,
        b.whcdl,
        CASE 
            WHEN TRIM(e.whcode) <> '' AND e.whcode IS NOT NULL THEN e.whcode
            ELSE b.whcdl
        END AS CompSiteId
    FROM 
        s_master.sale_orders b
    LEFT JOIN 
        (SELECT 
            referenceto, referencefrom, requestno, seqnum, exttax, seqline, whcode 
         FROM b_source.ord_site 
         WHERE delete_flag <> 'D' AND TRIM(whcode) != '' AND whcode IS NOT NULL
         
         UNION
         
         SELECT 
            referenceto, referencefrom, requestno, seqnum, exttax, seqline, whcode 
         FROM b_source.ord_addr 
         WHERE delete_flag <> 'D' AND TRIM(whcode) != '' AND whcode IS NOT NULL
         
         UNION
         
         SELECT 
            refth AS referenceto, 
            refnh AS referencefrom, 
            drqhx AS requestno, 
            seqnh AS seqnum, 
            extth AS exttax, 
            seqni AS seqline, 
            whcdi AS whcode 
         FROM b_source.ord_reg 
         WHERE delete_flag <> 'D' AND TRIM(whcdi) != '' AND whcdi IS NOT NULL) e
    ON 
        TRIM(b.OrderNumber) = TRIM(CONCAT(e.referenceto, e.referencefrom))
        AND b.requestnoc = e.requestno
        AND b.LineNumber = e.seqnum
        AND TRIM(b.exttax) = TRIM(e.exttax)
        AND b.seqline = e.seqline
)
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT OrderNumber) AS distinct_orders,
    SUM(CASE WHEN whcode IS NOT NULL THEN 1 ELSE 0 END) AS records_with_whcode,
    SUM(CASE WHEN whcdl IS NOT NULL THEN 1 ELSE 0 END) AS records_with_whcdl,
    SUM(CASE WHEN CompSiteId IS NOT NULL THEN 1 ELSE 0 END) AS records_with_site_id
FROM 
    site_data;