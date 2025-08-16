%sql
-- Create logging table if not exists
CREATE TABLE IF NOT EXISTS Finance.job_logs (
    job_name STRING,
    job_timestamp TIMESTAMP,
    records_processed BIGINT,
    errors_encountered BIGINT,
    error_details STRING,
    execution_time_seconds BIGINT,
    status STRING
);

-- Log the job execution metrics
INSERT INTO Finance.job_logs
SELECT 
    'Finance Data Processing Job' AS job_name,
    current_timestamp() AS job_timestamp,
    (SELECT COUNT(*) FROM Finance.finance WHERE LoadTimestamp > date_sub(current_timestamp(), 1)) AS records_processed,
    0 AS errors_encountered, -- This would be populated by the exception handling logic
    NULL AS error_details, -- This would be populated by the exception handling logic
    0 AS execution_time_seconds, -- This would be calculated in the actual job
    'COMPLETED' AS status;