"""
Module to update the ordersummary table when there are changes in the customer table.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
import logging
from datetime import datetime
from src.config import CUSTOMER_DELTA_TABLE, ORDERSUMMARY_DELTA_TABLE
from src.utils import setup_logger, read_delta_table, write_to_delta_table

logger = setup_logger("update_ordersummary")

def detect_customer_changes(spark: SparkSession) -> DataFrame:
    """
    Detect changes in the customer table by comparing with the customer data in ordersummary.
    
    Args:
        spark: The SparkSession
        
    Returns:
        DataFrame: The changed customer records
    """
    try:
        # Read current customer data
        current_customer_df = read_delta_table(CUSTOMER_DELTA_TABLE)
        
        # Read ordersummary table
        ordersummary_df = read_delta_table(ORDERSUMMARY_DELTA_TABLE)
        
        # Extract customer data from ordersummary
        customer_columns = [col for col in current_customer_df.columns]
        ordersummary_customer_df = ordersummary_df.select(*customer_columns).distinct()
        
        # Find changed records
        changed_records = current_customer_df.exceptAll(ordersummary_customer_df)
        
        logger.info(f"Detected {changed_records.count()} changed customer records")
        return changed_records
    except Exception as e:
        logger.error(f"Error detecting customer changes: {str(e)}")
        raise

def update_ordersummary_for_changes(spark: SparkSession, changed_customers: DataFrame) -> None:
    """
    Update ordersummary table for changed customer records.
    
    Args:
        spark: The SparkSession
        changed_customers: The DataFrame of changed customer records
    """
    try:
        if changed_customers.count() == 0:
            logger.info("No customer changes detected. No updates needed.")
            return
        
        # Read ordersummary table
        ordersummary_df = read_delta_table(ORDERSUMMARY_DELTA_TABLE)
        
        # Get current date for updates
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Get customer IDs that have changed
        changed_cust_ids = [row.CustId for row in changed_customers.select("CustId").distinct().collect()]
        
        # Update existing records (set EndDate and IsActive)
        spark.sql(f"""
            UPDATE {ORDERSUMMARY_DELTA_TABLE}
            SET EndDate = '{current_date}',
                IsActive = false,
                LastUpdated = current_timestamp()
            WHERE CustId IN ({','.join(map(str, changed_cust_ids))})
              AND IsActive = true
        """)
        
        logger.info(f"Updated {len(changed_cust_ids)} existing customer records in ordersummary")
        
        # Read order data for the changed customers
        order_data = spark.sql(f"""
            SELECT * FROM gen_ai_poc_databrickscoe.sdlc_wizard.order
            WHERE CustId IN ({','.join(map(str, changed_cust_ids))})
        """)
        
        # Join changed customers with their orders
        joined_df = changed_customers.join(order_data, "CustId", "inner")
        
        # Add SCD Type 2 columns
        new_records = joined_df.withColumn("StartDate", lit(current_date)) \
                              .withColumn("EndDate", lit("9999-12-31")) \
                              .withColumn("IsActive", lit(True)) \
                              .withColumn("LastUpdated", current_timestamp())
        
        # Insert new records
        new_records.write.format("delta").mode("append").saveAsTable(ORDERSUMMARY_DELTA_TABLE)
        
        logger.info(f"Inserted {new_records.count()} new records into ordersummary")
    except Exception as e:
        logger.error(f"Error updating ordersummary for changes: {str(e)}")
        raise

def main():
    """
    Main function to update ordersummary table when customer data changes.
    """
    try:
        spark = SparkSession.builder.appName("Update OrderSummary Table").getOrCreate()
        
        # Detect customer changes
        changed_customers = detect_customer_changes(spark)
        
        # Update ordersummary table for changes
        update_ordersummary_for_changes(spark, changed_customers)
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise
    finally:
        logger.info("OrderSummary table update process completed")

if __name__ == "__main__":
    main()