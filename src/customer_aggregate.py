"""
Module to create and update the customer aggregate spend table (TR-DELTA-006, TR-DELTA-007)
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

from src.config import (
    ORDER_SUMMARY_DELTA_TABLE,
    CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE,
    CUSTOMER_AGGREGATE_SPEND_SCHEMA
)
from src.utils import log_info, log_error, table_exists

def create_customer_aggregate_spend_table():
    """
    Create the customer aggregate spend table if it doesn't exist (TR-DELTA-006)
    """
    spark = SparkSession.builder.appName("CreateCustomerAggregateSpendTable").getOrCreate()
    
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Check if customer aggregate spend table exists
        if not table_exists(spark, CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE):
            log_info(f"Creating customer aggregate spend table: {CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE}")
            
            # Create empty table with schema
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE} (
                {CUSTOMER_AGGREGATE_SPEND_SCHEMA}
            ) USING DELTA
            """
            
            spark.sql(create_table_sql)
            log_info(f"Successfully created customer aggregate spend table: {CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE}")
        else:
            log_info(f"Customer aggregate spend table already exists: {CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE}")
            
    except Exception as e:
        log_error("Error creating customer aggregate spend table", e)
        raise

def load_customer_aggregate_spend_table():
    """
    Aggregate data from order summary table and load into customer aggregate spend table (TR-DELTA-007)
    """
    spark = SparkSession.builder.appName("LoadCustomerAggregateSpendTable").getOrCreate()
    
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Create customer aggregate spend table if it doesn't exist
        create_customer_aggregate_spend_table()
        
        # Read order summary data
        log_info(f"Reading order summary data from {ORDER_SUMMARY_DELTA_TABLE}")
        order_summary_df = spark.read.format("delta").table(ORDER_SUMMARY_DELTA_TABLE)
        
        # Filter for current records only
        current_records_df = order_summary_df.filter(col("is_current") == True)
        
        # Aggregate data by Name and Date
        log_info("Aggregating data by Name and Date")
        aggregated_df = current_records_df.groupBy("Name", "Date") \
            .agg(spark_sum("TotalAmount").alias("TotalSpend"))
        
        # Write aggregated data to customer aggregate spend table
        log_info(f"Writing aggregated data to {CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE}")
        aggregated_df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE)
        
        log_info(f"Successfully loaded data into customer aggregate spend table: {CUSTOMER_AGGREGATE_SPEND_DELTA_TABLE}")
            
    except Exception as e:
        log_error("Error loading customer aggregate spend table", e)
        raise

if __name__ == "__main__":
    create_customer_aggregate_spend_table()
    load_customer_aggregate_spend_table()