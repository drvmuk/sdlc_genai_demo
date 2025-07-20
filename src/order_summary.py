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
            customer_df["EmailId"],
            customer_df["Region"],
            order_df["OrderId"],
            order_df["ItemName"],
            order_df["PricePerUnit"],
            order_df["Qty"],
            order_df["TotalAmount"],
            order_df["Date"]
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
        else:
            print('Table exists')
            # Merge into existing table
            target_df = spark.table(target_table)
            
            # Identify records to update (where IsActive = true)
            target_df.createOrReplaceTempView("target_vw")
            joined_df.createOrReplaceTempView("source_vw")
            
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING source_vw AS source_vw
            ON target.CustId = source_vw.CustId AND target.OrderId = source_vw.OrderId AND target.IsActive = true
            WHEN MATCHED AND (
                target.Name != source_vw.Name OR
                target.Region != source_vw.Region OR
                target.EmailId != source_vw.EmailId
            ) THEN
                UPDATE SET 
                    EndDate = current_date(),
                    IsActive = false,
                    UpdatedDate = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, TotalAmount, Date, StartDate, EndDate, IsActive, InsertedDate, UpdatedDate)
                VALUES (source_vw.CustId, source_vw.Name, source_vw.EmailId, source_vw.Region, source_vw.OrderId, source_vw.ItemName, source_vw.PricePerUnit, source_vw.Qty, source_vw.TotalAmount, source_vw.Date,
                        source_vw.StartDate, source_vw.EndDate, source_vw.IsActive, source_vw.InsertedDate, source_vw.UpdatedDate)
            """    
            spark.sql(merge_sql)

            updated_records = joined_df.join(target_df, ["CustId", "Name", "EmailId", "Region", "OrderId"], "left_anti")
            
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

if __name__ == "__main__":
    main()
