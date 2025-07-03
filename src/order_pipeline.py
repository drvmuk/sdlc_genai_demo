"""
Main pipeline module for processing customer and order data
"""
from datetime import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, when, 
    concat, sha2, count, sum, datediff, year, month, 
    dayofmonth, expr
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from src.config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH,
    CUSTOMER_TABLE, ORDER_TABLE, ORDER_SUMMARY_TABLE,
    SCD_COLUMNS, MAX_RETRIES, RETRY_DELAY_SECONDS
)
from src.logger import get_logger, log_job_metrics

# Initialize logger
logger = get_logger("order_pipeline")

def create_spark_session():
    """
    Create and configure a Spark session
    
    Returns:
        SparkSession: Configured Spark session
    """
    spark = (SparkSession.builder
             .appName("Customer Order Pipeline")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    
    logger.info("Spark session created")
    return spark

def load_customer_data(spark):
    """
    Load customer data from CSV files and create a Delta table
    
    Args:
        spark: SparkSession
        
    Returns:
        DataFrame: Processed customer DataFrame
    """
    logger.info(f"Loading customer data from {CUSTOMER_DATA_PATH}")
    
    try:
        # Read customer data
        customer_df = (spark.read
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .csv(CUSTOMER_DATA_PATH))
        
        # Clean data - remove nulls and duplicates
        customer_df = (customer_df
                      .filter(col("CustId").isNotNull())
                      .filter(col("Name").isNotNull())
                      .dropDuplicates(["CustId"]))
        
        # Add metadata columns
        customer_df = (customer_df
                      .withColumn("load_date", current_timestamp())
                      .withColumn("load_id", lit(datetime.now().strftime("%Y%m%d%H%M%S"))))
        
        # Write to Delta table
        customer_df.write.format("delta").mode("overwrite").saveAsTable(CUSTOMER_TABLE)
        
        logger.info(f"Successfully loaded {customer_df.count()} customer records")
        return customer_df
        
    except Exception as e:
        logger.error(f"Error loading customer data: {str(e)}")
        raise

def load_order_data(spark):
    """
    Load order data from CSV files and create a Delta table
    
    Args:
        spark: SparkSession
        
    Returns:
        DataFrame: Processed order DataFrame
    """
    logger.info(f"Loading order data from {ORDER_DATA_PATH}")
    
    try:
        # Read order data
        order_df = (spark.read
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .csv(ORDER_DATA_PATH))
        
        # Clean data - remove nulls and duplicates
        order_df = (order_df
                   .filter(col("OrderId").isNotNull())
                   .filter(col("CustId").isNotNull())
                   .filter(col("Date").isNotNull())
                   .dropDuplicates(["OrderId"]))
        
        # Convert date string to date type if needed
        if order_df.schema["Date"].dataType.simpleString() == "string":
            order_df = order_df.withColumn("Date", to_date(col("Date")))
        
        # Add metadata columns
        order_df = (order_df
                   .withColumn("load_date", current_timestamp())
                   .withColumn("load_id", lit(datetime.now().strftime("%Y%m%d%H%M%S"))))
        
        # Write to Delta table
        order_df.write.format("delta").mode("overwrite").saveAsTable(ORDER_TABLE)
        
        logger.info(f"Successfully loaded {order_df.count()} order records")
        return order_df
        
    except Exception as e:
        logger.error(f"Error loading order data: {str(e)}")
        raise

def create_order_summary_scd2(spark, customer_df, order_df):
    """
    Create or update order summary SCD Type 2 table
    
    Args:
        spark: SparkSession
        customer_df: Customer DataFrame
        order_df: Order DataFrame
    """
    logger.info("Creating order summary SCD Type 2 table")
    
    try:
        # Calculate order summary
        summary_df = (order_df
                     .join(customer_df, "CustId")
                     .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
                     .withColumn("OrderYear", year(col("Date")))
                     .withColumn("OrderMonth", month(col("Date")))
                     .withColumn("OrderDay", dayofmonth(col("Date")))
                     .groupBy("CustId", "Name", "Region", "OrderYear", "OrderMonth")
                     .agg(
                         count("OrderId").alias("OrderCount"),
                         sum("TotalAmount").alias("TotalSpend"),
                         F.min("Date").alias("FirstOrderDate"),
                         F.max("Date").alias("LastOrderDate")
                     ))
        
        # Generate hash key for change detection
        summary_df = summary_df.withColumn(
            "hash_key", 
            sha2(concat(
                col("CustId"),
                col("Name"),
                col("Region"),
                col("OrderCount").cast("string"),
                col("TotalSpend").cast("string")
            ), 256)
        )
        
        # Add SCD Type 2 columns
        current_timestamp_val = datetime.now()
        summary_df = (summary_df
                     .withColumn(SCD_COLUMNS["effective_start_date"], lit(current_timestamp_val))
                     .withColumn(SCD_COLUMNS["effective_end_date"], lit("9999-12-31").cast("timestamp"))
                     .withColumn(SCD_COLUMNS["is_current"], lit(True))
                     .withColumn(SCD_COLUMNS["surrogate_key"], 
                                concat(col("CustId"), lit("_"), col("OrderYear"), lit("_"), col("OrderMonth"))))
        
        # Check if target table exists
        tables = spark.sql("SHOW TABLES IN gen_ai_poc_databrickscoe.sdlc_wizard").collect()
        table_exists = any(row.tableName == "ordersummary" for row in tables)
        
        if not table_exists:
            # Create new table if it doesn't exist
            logger.info("Order summary table doesn't exist. Creating new table.")
            summary_df.write.format("delta").mode("overwrite").saveAsTable(ORDER_SUMMARY_TABLE)
        else:
            # Implement SCD Type 2 logic
            logger.info("Order summary table exists. Implementing SCD Type 2 updates.")
            
            # Read current data
            current_df = spark.table(ORDER_SUMMARY_TABLE)
            
            # Identify new records (not in current table)
            new_records = summary_df.join(
                current_df.filter(col(SCD_COLUMNS["is_current"]) == True),
                summary_df[SCD_COLUMNS["surrogate_key"]] == current_df[SCD_COLUMNS["surrogate_key"]],
                "left_anti"
            )
            
            # Identify changed records
            joined_df = summary_df.alias("new").join(
                current_df.filter(col(SCD_COLUMNS["is_current"]) == True).alias("current"),
                summary_df[SCD_COLUMNS["surrogate_key"]] == current_df[SCD_COLUMNS["surrogate_key"]],
                "inner"
            )
            
            changed_records = joined_df.filter(col("new.hash_key") != col("current.hash_key"))
            
            # Get surrogate keys for changed records
            changed_sks = changed_records.select(col("current." + SCD_COLUMNS["surrogate_key"]))
            
            # Update existing records (set end date and current flag)
            if changed_sks.count() > 0:
                spark.sql(f"""
                UPDATE {ORDER_SUMMARY_TABLE}
                SET {SCD_COLUMNS["effective_end_date"]} = CURRENT_TIMESTAMP(),
                    {SCD_COLUMNS["is_current"]} = false
                WHERE {SCD_COLUMNS["surrogate_key"]} IN 
                    (SELECT {SCD_COLUMNS["surrogate_key"]} FROM delta.`{changed_sks}`)
                    AND {SCD_COLUMNS["is_current"]} = true
                """)
            
            # Extract changed records from new dataframe
            changed_new_records = summary_df.join(
                changed_sks,
                summary_df[SCD_COLUMNS["surrogate_key"]] == changed_sks[SCD_COLUMNS["surrogate_key"]],
                "inner"
            )
            
            # Combine new and changed records
            records_to_insert = new_records.union(changed_new_records)
            
            # Insert new records
            if records_to_insert.count() > 0:
                records_to_insert.write.format("delta").mode("append").saveAsTable(ORDER_SUMMARY_TABLE)
        
        logger.info("Successfully created/updated order summary SCD Type 2 table")
        
    except Exception as e:
        logger.error(f"Error creating order summary SCD Type 2 table: {str(e)}")
        raise

def execute_with_retry(func, *args, **kwargs):
    """
    Execute a function with retry logic
    
    Args:
        func: Function to execute
        *args: Function arguments
        **kwargs: Function keyword arguments
        
    Returns:
        Result of the function execution
    """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            retries += 1
            if retries >= MAX_RETRIES:
                logger.error(f"Maximum retries reached. Last error: {str(e)}")
                raise
            
            logger.warning(f"Attempt {retries} failed: {str(e)}. Retrying in {RETRY_DELAY_SECONDS} seconds...")
            time.sleep(RETRY_DELAY_SECONDS)

def main():
    """
    Main pipeline execution function
    """
    start_time = datetime.now()
    log_job_metrics(logger, "Customer Order Pipeline", start_time)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Load customer data
        customer_df = execute_with_retry(load_customer_data, spark)
        
        # Load order data
        order_df = execute_with_retry(load_order_data, spark)
        
        # Create order summary SCD Type 2 table
        execute_with_retry(create_order_summary_scd2, spark, customer_df, order_df)
        
        # Log successful completion
        end_time = datetime.now()
        total_records = customer_df.count() + order_df.count()
        log_job_metrics(logger, "Customer Order Pipeline", start_time, end_time, total_records, "Completed")
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        # Log failure
        end_time = datetime.now()
        log_job_metrics(logger, "Customer Order Pipeline", start_time, end_time, None, f"Failed: {str(e)}")
        
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()