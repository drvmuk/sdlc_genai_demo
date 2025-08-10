"""
Order summary module for creating and updating the order summary table
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp, col, max as max_
from datetime import datetime, date
import logging

from src.config import CUSTOMER_TABLE, ORDER_TABLE, ORDER_SUMMARY_TABLE
from src.utils import get_spark_session, table_exists, log_info, log_error

def create_order_summary_table():
    """
    Create the order summary table if it does not exist
    """
    spark = get_spark_session()
    
    try:
        log_info(f"Checking if {ORDER_SUMMARY_TABLE} exists")
        
        # Extract database and table name
        db_name, schema_name, table_name = ORDER_SUMMARY_TABLE.split(".")
        
        # Check if the table exists
        tables = spark.sql(f"SHOW TABLES IN {db_name}.{schema_name}")
        if tables.filter(tables.tableName == table_name).count() == 0:
            log_info(f"{ORDER_SUMMARY_TABLE} does not exist, creating it")
            
            # Create the table with SCD Type 2 schema
            spark.sql(f"""
                CREATE TABLE {ORDER_SUMMARY_TABLE} (
                    OrderId INT,
                    ItemName STRING,
                    PricePerUnit DOUBLE,
                    Qty INT,
                    OrderDate DATE,
                    CustId INT,
                    Name STRING,
                    EmailId STRING,
                    Region STRING,
                    TotalPrice DOUBLE,
                    StartDate TIMESTAMP,
                    EndDate TIMESTAMP,
                    IsActive BOOLEAN
                )
                USING DELTA
            """)
            
            log_info(f"{ORDER_SUMMARY_TABLE} created successfully")
        else:
            log_info(f"{ORDER_SUMMARY_TABLE} already exists")
            
    except Exception as e:
        log_error(f"Error creating {ORDER_SUMMARY_TABLE}", e)
        raise

def load_order_summary_data():
    """
    Join customer and order data and load it into the order summary table
    """
    spark = get_spark_session()
    
    try:
        log_info("Starting to load order summary data")
        
        # Read customer and order data
        customer_df = spark.read.format("delta").table(CUSTOMER_TABLE)
        order_df = spark.read.format("delta").table(ORDER_TABLE)
        
        log_info(f"Read {customer_df.count()} customer records and {order_df.count()} order records")
        
        # Join customer and order data
        joined_df = order_df.join(
            customer_df,
            order_df.CustId == customer_df.CustId,
            "inner"
        ).select(
            order_df.OrderId,
            order_df.ItemName,
            order_df.PricePerUnit,
            order_df.Qty,
            order_df.Date.alias("OrderDate"),
            customer_df.CustId,
            customer_df.Name,
            customer_df.EmailId,
            customer_df.Region,
            (order_df.PricePerUnit * order_df.Qty).alias("TotalPrice")
        )
        
        log_info(f"Joined data has {joined_df.count()} records")
        
        # Add SCD Type 2 columns
        current_time = current_timestamp()
        max_date = lit(date(9999, 12, 31).strftime("%Y-%m-%d"))
        
        scd_df = joined_df.withColumn("StartDate", current_time) \
                         .withColumn("EndDate", max_date) \
                         .withColumn("IsActive", lit(True))
        
        # Write to order summary table
        scd_df.write.format("delta") \
                  .mode("overwrite") \
                  .saveAsTable(ORDER_SUMMARY_TABLE)
        
        log_info(f"Successfully loaded data into {ORDER_SUMMARY_TABLE}")
        
    except Exception as e:
        log_error("Error loading order summary data", e)
        raise

def update_order_summary():
    """
    Update the order summary table when customer data changes
    """
    spark = get_spark_session()
    
    try:
        log_info("Starting to update order summary data")
        
        # Read current customer data
        customer_df = spark.read.format("delta").table(CUSTOMER_TABLE)
        
        # Read current order summary data
        order_summary_df = spark.read.format("delta").table(ORDER_SUMMARY_TABLE)
        
        # Find active records
        active_records = order_summary_df.filter(col("IsActive") == True)
        
        # Join to find changed records
        changed_records = active_records.join(
            customer_df,
            (active_records.CustId == customer_df.CustId) &
            ((active_records.Name != customer_df.Name) |
             (active_records.EmailId != customer_df.EmailId) |
             (active_records.Region != customer_df.Region)),
            "inner"
        ).select(active_records["*"])
        
        if changed_records.count() > 0:
            log_info(f"Found {changed_records.count()} changed records")
            
            # Update end date and is active flag for changed records
            current_time = current_timestamp()
            
            # Expire old records
            expired_records = changed_records.withColumn("EndDate", current_time) \
                                          .withColumn("IsActive", lit(False))
            
            # Create new records with updated customer data
            new_records = active_records.join(
                customer_df,
                active_records.CustId == customer_df.CustId,
                "inner"
            ).select(
                active_records.OrderId,
                active_records.ItemName,
                active_records.PricePerUnit,
                active_records.Qty,
                active_records.OrderDate,
                customer_df.CustId,
                customer_df.Name,
                customer_df.EmailId,
                customer_df.Region,
                active_records.TotalPrice,
                current_time.alias("StartDate"),
                lit(date(9999, 12, 31).strftime("%Y-%m-%d")).alias("EndDate"),
                lit(True).alias("IsActive")
            ).join(
                changed_records,
                "OrderId",
                "inner"
            )
            
            # Combine unchanged records, expired records, and new records
            unchanged_records = active_records.join(
                changed_records,
                "OrderId",
                "leftanti"
            )
            
            inactive_records = order_summary_df.filter(col("IsActive") == False)
            
            updated_df = unchanged_records.union(expired_records).union(new_records).union(inactive_records)
            
            # Write updated data back to the order summary table
            updated_df.write.format("delta") \
                       .mode("overwrite") \
                       .saveAsTable(ORDER_SUMMARY_TABLE)
            
            log_info(f"Successfully updated {ORDER_SUMMARY_TABLE}")
        else:
            log_info("No changes detected in customer data")
        
    except Exception as e:
        log_error("Error updating order summary data", e)
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    create_order_summary_table()
    load_order_summary_data()
    update_order_summary()