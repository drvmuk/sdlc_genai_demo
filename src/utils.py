from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session():
    """
    Get or create a SparkSession
    
    Returns:
        SparkSession: The current SparkSession
    """
    return SparkSession.builder.getOrCreate()

def validate_data(df, table_name):
    """
    Validate data quality for a DataFrame
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table for logging purposes
        
    Returns:
        tuple: (is_valid, validation_errors)
    """
    validation_errors = []
    
    # Check for null values
    null_counts = {}
    for column in df.columns:
        null_count = df.filter(F.col(column).isNull()).count()
        if null_count > 0:
            null_counts[column] = null_count
    
    if null_counts:
        validation_errors.append(f"Null values found in columns: {null_counts}")
    
    # Check for duplicate records
    total_rows = df.count()
    distinct_rows = df.distinct().count()
    
    if total_rows != distinct_rows:
        validation_errors.append(f"Found {total_rows - distinct_rows} duplicate rows")
    
    is_valid = len(validation_errors) == 0
    
    # Log validation results
    if is_valid:
        logger.info(f"Data validation passed for {table_name}")
    else:
        logger.warning(f"Data validation failed for {table_name}: {validation_errors}")
    
    return is_valid, validation_errors

def log_pipeline_metrics(pipeline_name, start_time, end_time, records_processed, success=True, error_message=None):
    """
    Log pipeline execution metrics
    
    Args:
        pipeline_name: Name of the pipeline
        start_time: Pipeline start time
        end_time: Pipeline end time
        records_processed: Number of records processed
        success: Whether the pipeline executed successfully
        error_message: Error message if pipeline failed
    """
    duration_seconds = (end_time - start_time).total_seconds()
    
    metrics = {
        "pipeline_name": pipeline_name,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration_seconds,
        "records_processed": records_processed,
        "success": success
    }
    
    if not success and error_message:
        metrics["error_message"] = error_message
    
    logger.info(f"Pipeline metrics: {metrics}")
    
    # In a production environment, you might want to store these metrics in a table
    # spark = get_spark_session()
    # metrics_df = spark.createDataFrame([metrics])
    # metrics_df.write.format("delta").mode("append").saveAsTable("pipeline_metrics")