"""
Module for creating and updating the order summary table.
Implements TR-DTLD-002 and TR-DTLD-003.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, current_date, to_date, expr, when
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_order_summary(spark, catalog, schema):
    """
    Create and load ordersummary table with joined data from customer and order tables
    using SCD type 2 logic.
    
    Args:
        spark: SparkSession
        catalog: Target catalog name
        schema: Target schema name
    """
    try:
        logger.info("Creating order summary table")
        
        # Read source tables
        customer_table = f"{catalog}.{schema}.customer"
        order_table = f"{catalog}.{schema}.order"
        target_table = f"{catalog}.{schema}.ordersummary"
        
        customer_df = spark.table(customer_table)
        order_df = spark.table(order_table)
        
        # Join customer and order data
        joined_df = order_df.join(
            customer_df,
            order_df["CustId"] == customer_df["CustId"],
            "inner"
        ).select(
            customer_df["CustId"],
            customer_df["Name"],
            customer_df["Address"],
            customer_df["Phone"],
            order_df["OrderId"],
            order_df["Date"],
            order_df["TotalAmount"],
            order_df["Status"]
        )
        
        # Add SCD Type 2 columns
        current_date_val = datetime.now().strftime("%Y-%m-%d")
        joined_df = joined_df \
            .withColumn("StartDate", to_date(lit(current_date_val))) \
            .withColumn("EndDate", to_date(lit("9999-12-31"))) \
            .withColumn("IsActive", lit(True)) \
            .withColumn("InsertedDate", current_timestamp()) \
            .withColumn("UpdatedDate", current_timestamp())
        
        # Check if target table exists
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").filter(col("tableName") == "ordersummary")
        
        if tables.count() == 0:
            # Create new table
            joined_df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(target_table)
            logger.info(f"Created new order summary table: {target_table}")
        else:
            # Merge into existing table
            target_df = spark.table(target_table)
            
            # Identify records to update (where IsActive = true)
            target_df.createOrReplaceTempView("target")
            joined_df.createOrReplaceTempView("source")
            
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING source AS source
            ON target.CustId = source.CustId AND target.OrderId = source.OrderId AND target.IsActive = true
            WHEN MATCHED AND (
                target.Name != source.Name OR
                target.Address != source.Address OR
                target.Phone != source.Phone OR
                target.Date != source.Date OR
                target.TotalAmount != source.TotalAmount OR
                target.Status != source.Status
            ) THEN
                UPDATE SET 
                    EndDate = current_date(),
                    IsActive = false,
                    UpdatedDate = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (CustId, Name, Address, Phone, OrderId, Date, TotalAmount, Status, StartDate, EndDate, IsActive, InsertedDate, UpdatedDate)
                VALUES (source.CustId, source.Name, source.Address, source.Phone, source.OrderId, source.Date, source.TotalAmount, source.Status, 
                        source.StartDate, source.EndDate, source.IsActive, source.InsertedDate, source.UpdatedDate)
            """
            
            spark.sql(merge_sql)
            
            # Insert new versions of updated records
            updated_records = spark.sql(f"""
                SELECT t.CustId, s.Name, s.Address, s.Phone, t.OrderId, s.Date, s.TotalAmount, s.Status,
                       current_date() as StartDate, to_date('9999-12-31') as EndDate, true as IsActive,
                       current_timestamp() as InsertedDate, current_timestamp() as UpdatedDate
                FROM {target_table} t
                JOIN source s ON t.CustId = s.CustId AND t.OrderId = s.OrderId
                WHERE t.EndDate = current_date() AND t.IsActive = false
            """)
            
            if updated_records.count() > 0:
                updated_records.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(target_table)
            
            logger.info(f"Updated order summary table: {target_table}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error creating order summary: {str(e)}")
        raise

def update_order_summary_on_customer_change(spark, catalog, schema):
    """
    Update ordersummary table when there are changes in the customer table.
    
    Args:
        spark: SparkSession
        catalog: Target catalog name
        schema: Target schema name
    """
    try:
        logger.info("Updating order summary based on customer changes")
        
        customer_table = f"{catalog}.{schema}.customer"
        order_summary_table = f"{catalog}.{schema}.ordersummary"
        
        # Get current active records from order summary
        order_summary_df = spark.table(order_summary_table).filter(col("IsActive") == True)
        customer_df = spark.table(customer_table)
        
        # Find changed customer records
        order_summary_df.createOrReplaceTempView("order_summary")
        customer_df.createOrReplaceTempView("customer")
        
        changed_customers = spark.sql(f"""
            SELECT DISTINCT os.CustId
            FROM order_summary os
            JOIN customer c ON os.CustId = c.CustId
            WHERE os.IsActive = true AND (
                os.Name != c.Name OR
                os.Address != c.Address OR
                os.Phone != c.Phone
            )
        """)
        
        if changed_customers.count() > 0:
            # Update existing records (set EndDate and IsActive)
            spark.sql(f"""
                UPDATE {order_summary_table}
                SET EndDate = current_date(),
                    IsActive = false,
                    UpdatedDate = current_timestamp()
                WHERE CustId IN (SELECT CustId FROM changed_customers)
                  AND IsActive = true
            """)
            
            # Insert new records with updated customer information
            spark.sql(f"""
                INSERT INTO {order_summary_table}
                SELECT 
                    c.CustId,
                    c.Name,
                    c.Address,
                    c.Phone,
                    os.OrderId,
                    os.Date,
                    os.TotalAmount,
                    os.Status,
                    current_date() as StartDate,
                    to_date('9999-12-31') as EndDate,
                    true as IsActive,
                    current_timestamp() as InsertedDate,
                    current_timestamp() as UpdatedDate
                FROM {order_summary_table} os
                JOIN {customer_table} c ON os.CustId = c.CustId
                WHERE os.CustId IN (SELECT CustId FROM changed_customers)
                  AND os.EndDate = current_date()
                  AND os.IsActive = false
            """)
            
            logger.info(f"Updated {changed_customers.count()} customers in order summary table")
        else:
            logger.info("No customer changes detected")
        
        return True
    
    except Exception as e:
        logger.error(f"Error updating order summary on customer change: {str(e)}")
        raise

def main():
    """Main function to execute order summary creation and update"""
    try:
        spark = SparkSession.builder \
            .appName("Order Summary Processing") \
            .getOrCreate()
        
        # Configuration
        catalog = "gen_ai_poc_databrickscoe"
        schema = "sdlc_wizard"
        
        # Create or update order summary
        create_order_summary(spark, catalog, schema)
        
        # Update order summary based on customer changes
        update_order_summary_on_customer_change(spark, catalog, schema)
        
        logger.info("Order summary processing completed successfully")
    
    except Exception as e:
        logger.error(f"Error in order summary processing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()