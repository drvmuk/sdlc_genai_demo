"""
SCD Type 2 updater for handling changes in customer data.
Updates the ordersummary table when customer data changes.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, col, when, coalesce
)
import datetime
from delta.tables import DeltaTable
import sys
import traceback

from src.config import (
    CUSTOMER_TABLE_PATH, ORDER_TABLE_PATH, ORDER_SUMMARY_TABLE_PATH,
    EFFECTIVE_FROM_COL, EFFECTIVE_TO_COL, CURRENT_FLAG_COL, INFINITE_DATE
)
from src.logger import get_logger, log_job_start, log_job_end, log_step

# Initialize logger
logger = get_logger(__name__)

def create_spark_session():
    """
    Create and return a Spark session configured for Delta Lake.
    
    Returns:
        SparkSession: Configured Spark session
    """
    return (SparkSession.builder
            .appName("SCD Type 2 Data Updater")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def update_scd_type2_table(spark):
    """
    Update the SCD Type 2 table based on changes in the customer table.
    
    Args:
        spark: SparkSession
    """
    log_step(logger, "Updating SCD Type 2 table based on customer changes")
    
    # Read current data
    customer_df = spark.table(CUSTOMER_TABLE_PATH)
    order_df = spark.table(ORDER_TABLE_PATH)
    
    # Join current customer and order data
    current_data = customer_df.join(order_df, "CustId", "inner")
    
    # Get the ordersummary Delta table
    try:
        order_summary_delta = DeltaTable.forName(spark, ORDER_SUMMARY_TABLE_PATH)
    except Exception as e:
        logger.error(f"Error accessing ordersummary table: {str(e)}")
        raise
    
    # Get current ordersummary data
    order_summary_df = order_summary_delta.toDF()
    
    # Identify the keys for matching records
    join_condition = """
        current_data.CustId = ordersummary.CustId AND
        ordersummary.{current_flag_col} = true
    """.format(current_flag_col=CURRENT_FLAG_COL)
    
    # Identify changed records
    matched_data = current_data.join(
        order_summary_df.alias("ordersummary"),
        join_condition,
        "left_outer"
    )
    
    # Get the current timestamp
    current_time = datetime.datetime.now()
    
    # Prepare new records to be inserted
    new_records = (current_data
                  .withColumn(EFFECTIVE_FROM_COL, current_timestamp())
                  .withColumn(EFFECTIVE_TO_COL, lit(INFINITE_DATE))
                  .withColumn(CURRENT_FLAG_COL, lit(True)))
    
    # Execute SCD Type 2 merge operation
    (order_summary_delta.alias("ordersummary")
     .merge(
         new_records.alias("updates"),
         "ordersummary.CustId = updates.CustId"
     )
     .whenMatchedUpdate(
         condition="ordersummary.{current_flag_col} = true AND (" +
                  " OR ".join([f"ordersummary.{col} <> updates.{col}" 
                              for col in customer_df.columns if col != "CustId"]) +
                  ")",
         set={
             EFFECTIVE_TO_COL: current_timestamp(),
             CURRENT_FLAG_COL: lit(False)
         }
     )
     .whenNotMatchedInsertAll()
     .execute())
    
    # Insert new versions of changed records
    changed_records = (order_summary_delta.toDF()
                      .filter(f"{CURRENT_FLAG_COL} = false AND {EFFECTIVE_TO_COL} = '{current_time}'")
                      .join(current_data, "CustId", "inner")
                      .select(current_data["*"], 
                             current_timestamp().alias(EFFECTIVE_FROM_COL),
                             lit(INFINITE_DATE).alias(EFFECTIVE_TO_COL),
                             lit(True).alias(CURRENT_FLAG_COL)))
    
    # Append the changed records with new versions
    if changed_records.count() > 0:
        changed_records.write.format("delta").mode("append").saveAsTable(ORDER_SUMMARY_TABLE_PATH)
        logger.info(f"Updated {changed_records.count()} records with new versions")
    else:
        logger.info("No changes detected in customer data")

def main():
    """
    Main function to execute the SCD Type 2 update process.
    """
    log_job_start(logger, "SCD Type 2 Update Process")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Update SCD Type 2 table
        update_scd_type2_table(spark)
        
        log_job_end(logger, "SCD Type 2 Update Process")
        
    except Exception as e:
        logger.error(f"Error in SCD Type 2 update process: {str(e)}")
        logger.error(traceback.format_exc())
        log_job_end(logger, "SCD Type 2 Update Process", success=False)
        sys.exit(1)

if __name__ == "__main__":
    main()