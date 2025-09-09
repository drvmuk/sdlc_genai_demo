"""
Delta Live Tables pipeline for customer and order data processing.
"""

import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

from src.config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH, CATALOG, SCHEMA,
    CUSTOMER_TABLE, ORDER_TABLE, ORDER_SUMMARY_TABLE, CUSTOMER_AGGREGATE_SPEND_TABLE
)

# Step 1: Read source data and create bronze tables
@dlt.table(
    name=f"{CUSTOMER_TABLE}_bronze",
    comment="Bronze layer for customer data"
)
def customer_bronze():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(CUSTOMER_DATA_PATH)

@dlt.table(
    name=f"{ORDER_TABLE}_bronze",
    comment="Bronze layer for order data"
)
def order_bronze():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(ORDER_DATA_PATH)

# Step 2-4: Clean data and create silver tables
@dlt.table(
    name=CUSTOMER_TABLE,
    comment="Silver layer for customer data with nulls and duplicates removed"
)
def customer_silver():
    return dlt.read(f"{CUSTOMER_TABLE}_bronze") \
        .na.drop() \
        .dropDuplicates()

@dlt.table(
    name=ORDER_TABLE,
    comment="Silver layer for order data with TotalAmount calculated and nulls/duplicates removed"
)
def order_silver():
    return dlt.read(f"{ORDER_TABLE}_bronze") \
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty")) \
        .na.drop() \
        .dropDuplicates()

# Step 6-8: Create SCD Type 2 table
@dlt.table(
    name=ORDER_SUMMARY_TABLE,
    comment="Gold layer for order summary with SCD Type 2 tracking"
)
@dlt.expect_or_drop("ValidCustId", "CustId IS NOT NULL")
def order_summary():
    # Get current data
    customer_data = dlt.read(CUSTOMER_TABLE)
    order_data = dlt.read(ORDER_TABLE)
    
    # Join customer and order data
    joined_df = order_data.join(customer_data, "CustId")
    
    # Add SCD Type 2 columns for new data
    current_time = current_timestamp()
    new_data = joined_df.withColumn("IsActive", lit(True)) \
                       .withColumn("StartDate", current_time) \
                       .withColumn("EndDate", lit(None).cast(TimestampType()))
    
    # Check if the target table exists
    try:
        # Read existing data
        existing_data = dlt.read(ORDER_SUMMARY_TABLE)
        
        # Find records that need to be updated (customer data changed)
        records_to_update = existing_data.alias("existing").join(
            new_data.alias("new"),
            (col("existing.CustId") == col("new.CustId")) &
            (col("existing.OrderId") == col("new.OrderId")) &
            col("existing.IsActive") &
            (
                (col("existing.Name") != col("new.Name")) |
                (col("existing.EmailId") != col("new.EmailId")) |
                (col("existing.Region") != col("new.Region"))
            ),
            "inner"
        ).select("existing.*")
        
        # Mark old records as inactive
        updated_records = records_to_update.withColumn("IsActive", lit(False)) \
                                          .withColumn("EndDate", current_time)
        
        # Identify records to keep unchanged
        records_to_keep = existing_data.alias("existing").join(
            records_to_update.alias("update"),
            (col("existing.CustId") == col("update.CustId")) &
            (col("existing.OrderId") == col("update.OrderId")) &
            (col("existing.IsActive") == col("update.IsActive")),
            "left_anti"
        ).select("existing.*")
        
        # Union all records together
        return records_to_keep.union(updated_records).union(new_data)
    except:
        # Table doesn't exist yet, return new data
        return new_data

# Step 9-10: Create customer aggregate spend table
@dlt.table(
    name=CUSTOMER_AGGREGATE_SPEND_TABLE,
    comment="Gold layer for customer aggregate spend"
)
def customer_aggregate_spend():
    # Read from order summary table
    order_summary_df = dlt.read(ORDER_SUMMARY_TABLE)
    
    # Aggregate by customer name and date
    return order_summary_df.filter(col("IsActive") == True) \
                          .groupBy("Name", "Date") \
                          .sum("TotalAmount") \
                          .withColumnRenamed("sum(TotalAmount)", "TotalAmount") \
                          .select("Name", "TotalAmount", "Date")