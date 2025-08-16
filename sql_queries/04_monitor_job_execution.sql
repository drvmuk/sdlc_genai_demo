%sql
-- Monitor job execution history
SELECT 
    job_name,
    job_timestamp,
    records_processed,
    errors_encountered,
    status,
    execution_time_seconds
FROM 
    Finance.job_logs
WHERE 
    job_name = 'Finance Data Processing Job'
ORDER BY 
    job_timestamp DESC
LIMIT 10;