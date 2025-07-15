"""
Module to create and update the order summary table (TR-DELTA-003, TR-DELTA-004, TR-DELTA-005)
"""
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date

from src.config import (
    CUSTOMER_DELTA_TABLE,
    ORDER_DELTA_TABLE,
    ORDER_SUMMARY_DELTA_TABLE,
    ORDER_SUMMARY_SCHEMA
)
from src.utils import log_info, log_error, table_exists

def create_order_summary_table():
    """
    Create the order summary table if it doesn't exist (TR-DELTA-003)
    """
    spark = SparkSession.builder.appName("CreateOrderSummaryTable").getOrCreate()
    
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Check if order summary table exists
        if not table_exists(spark, ORDER_SUMMARY_DELTA_TABLE):
            log_info(f"Creating order summary table: {ORDER_SUMMARY_DELTA_TABLE}")
            
            # Create empty table with schema
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {ORDER_SUMMARY_DELTA_TABLE} (
                {ORDER_SUMMARY_SCHEMA}
            ) USING DELTA
            """
            
            spark.sql(create_table_sql)
            log_info(f"Successfully created order summary table: {ORDER_SUMMARY_DELTA_TABLE}")
        else:
            log_info(f"Order summary table already exists: {ORDER_SUMMARY_DELTA_TABLE}")
            
    except Exception as e:
        log_error("Error creating order summary table", e)
        raise

def load_order_summary_table():
    """
    Join customer and order data and load into order summary table (TR-DELTA-004)
    """
    spark = SparkSession.builder.appName("LoadOrderSummaryTable").getOrCreate()
    
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Create order summary table if it doesn't exist
        create_order_summary_table()
        
        # Read customer and order data
        log_info("Reading customer and order data")
        customer_df = spark.read.format("delta").table(CUSTOMER_DELTA_TABLE)
        order_df = spark.read.format("delta").table(ORDER_DELTA_TABLE)
        
        # Join customer and order data
        log_info("Joining customer and order data")
        joined_df = customer_df.join(order_df, "CustId", "inner") \
            .select(
                customer_df["CustId"],
                customer_df["Name"],
                customer_df["Address"],
                customer_df["Phone"],
                order_df["OrderId"],
                to_date(order_df["Date"]).alias("Date"),
                order_df["TotalAmount"],
                current_timestamp().alias("effective_start_date"),
                lit(None).cast("timestamp").alias("effective_end_date"),
                lit(True).alias("is_current")
            )
        
        # Load data into order summary table
        log_info(f"Loading data into order summary table: {ORDER_SUMMARY_DELTA_TABLE}")
        
        # Check if the table is empty
        count = spark.read.format("delta").table(ORDER_SUMMARY_DELTA_TABLE).count()
        
        if count == 0:
            # If table is empty, just write the data
            joined_df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(ORDER_SUMMARY_DELTA_TABLE)
        else:
            # If table has data, merge with SCD Type 2 logic
            # This will be handled by update_order_summary_on_customer_change
            pass
        
        log_info(f"Successfully loaded data into order summary table: {ORDER_SUMMARY_DELTA_TABLE}")
            
    except Exception as e:
        log_error("Error loading order summary table", e)
        raise

def update_order_summary_on_customer_change():
    """
    Update order summary table when customer data changes (TR-DELTA-005)
    """
    spark = SparkSession.builder.appName("UpdateOrderSummary").getOrCreate()
    
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Read current data
        log_info("Reading customer, order, and order summary data")
        customer_df = spark.read.format("delta").table(CUSTOMER_DELTA_TABLE)
        order_df = spark.read.format("delta").table(ORDER_DELTA_TABLE)
        
        # Check if order summary table exists and has data
        if not table_exists(spark, ORDER_SUMMARY_DELTA_TABLE):
            log_info("Order summary table does not exist, creating and loading data")
            load_order_summary_table()
            return
            
        # Read current order summary
        order_summary_df = spark.read.format("delta").table(ORDER_SUMMARY_DELTA_TABLE)
        
        # Create new joined data
        log_info("Creating new joined data")
        new_joined_df = customer_df.join(order_df, "CustId", "inner") \
            .select(
                customer_df["CustId"],
                customer_df["Name"],
                customer_df["Address"],
                customer_df["Phone"],
                order_df["OrderId"],
                to_date(order_df["Date"]).alias("Date"),
                order_df["TotalAmount"]
            )
        
        # Register as temp view for SQL operations
        new_joined_df.createOrReplaceTempView("new_joined_data")
        order_summary_df.createOrReplaceTempView("current_order_summary")
        
        # Identify changes using merge
        log_info("Identifying changes and updating order summary table")
        
        # Execute merge operation for SCD Type 2
        merge_sql = f"""
        MERGE INTO {ORDER_SUMMARY_DELTA_TABLE} target
        USING (
            SELECT 
                n.CustId, n.Name, n.Address, n.Phone, n.OrderId, n.Date, n.TotalAmount
            FROM new_joined_data n
        ) source
        ON target.OrderId = source.OrderId AND target.is_current = true
        WHEN MATCHED AND (
            target.Name != source.Name OR 
            target.Address != source.Address OR 
            target.Phone != source.Phone
        ) THEN
            UPDATE SET 
                is_current = false,
                effective_end_date = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (
                CustId, Name, Address, Phone, OrderId, Date, TotalAmount, 
                effective_start_date, effective_end_date, is_current
            )
            VALUES (
                source.CustId, source.Name, source.Address, source.Phone, source.OrderId, source.Date, source.TotalAmount,
                current_timestamp(), NULL, true
            )
        """
        
        spark.sql(merge_sql)
        
        # Insert new records for updated rows
        insert_sql = f"""
        INSERT INTO {ORDER_SUMMARY_DELTA_TABLE} (
            CustId, Name, Address, Phone, OrderId, Date, TotalAmount, 
            effective_start_date, effective_end_date, is_current
        )
        SELECT 
            n.CustId, n.Name, n.Address, n.Phone, n.OrderId, n.Date, n.TotalAmount,
            current_timestamp(), NULL, true
        FROM new_joined_data n
        JOIN current_order_summary c
        ON n.OrderId = c.OrderId
        WHERE c.is_current = false
        AND c.effective_end_date = (
            SELECT MAX(effective_end_date) 
            FROM current_order_summary 
            WHERE OrderId = c.OrderId AND is_current = false
        )
        """
        
        spark.sql(insert_sql)
        
        log_info(f"Successfully updated order summary table: {ORDER_SUMMARY_DELTA_TABLE}")
            
    except Exception as e:
        log_error("Error updating order summary table", e)
        raise

if __name__ == "__main__":
    create_order_summary_table()
    load_order_summary_table()
    update_order_summary_on_customer_change()