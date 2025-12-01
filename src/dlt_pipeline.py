"""
Delta Live Tables pipeline for customer and order data processing with SCD Type 2 implementation.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, BooleanType, TimestampType

from config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH, 
    TARGET_CATALOG, TARGET_SCHEMA,
    CUSTOMER_TABLE, ORDER_TABLE, ORDER_SUMMARY_TABLE, CUSTOMER_AGGREGATE_SPEND_TABLE
)

# Define schemas
customer_schema = StructType([
    StructField("CustId", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("EmailId", StringType(), True),
    StructField("Region", StringType(), True)
])

order_schema = StructType([
    StructField("OrderId", StringType(), True),
    StructField("ItemName", StringType(), True),
    StructField("PricePerUnit", DoubleType(), True),
    StructField("Qty", IntegerType(), True),
    StructField("Date", DateType(), True),
    StructField("CustId", StringType(), True)
])

# Step 1: Read source CSV data and load to bronze tables
@dlt.table(
    name=f"{CUSTOMER_TABLE}_bronze",
    comment="Raw customer data from CSV files"
)
def customer_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(customer_schema)
        .load(CUSTOMER_DATA_PATH)
    )

@dlt.table(
    name=f"{ORDER_TABLE}_bronze",
    comment="Raw order data from CSV files"
)
def order_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_schema)
        .load(ORDER_DATA_PATH)
    )

# Step 2 & 4: Clean customer data (remove nulls and duplicates)
@dlt.table(
    name=CUSTOMER_TABLE,
    comment="Cleaned customer data"
)
def customer_silver():
    return (
        dlt.read(f"{CUSTOMER_TABLE}_bronze")
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
        .dropDuplicates(["CustId"])
    )

# Step 3 & 4: Clean order data and add TotalAmount column
@dlt.table(
    name=ORDER_TABLE,
    comment="Cleaned order data with TotalAmount calculated"
)
def order_silver():
    return (
        dlt.read(f"{ORDER_TABLE}_bronze")
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .dropDuplicates(["OrderId"])
    )

# Step 5, 6, 7, 8: Create SCD Type 2 order summary table
@dlt.table(
    name=ORDER_SUMMARY_TABLE,
    comment="Order summary with customer details as SCD Type 2",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_all_or_drop({"valid_custid": "CustId IS NOT NULL"})
def order_summary():
    # Get current data in the target table if it exists
    try:
        existing_data = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{ORDER_SUMMARY_TABLE}")
        has_existing_data = True
    except:
        has_existing_data = False
    
    # Get the current customer and order data
    customer_df = dlt.read(CUSTOMER_TABLE)
    order_df = dlt.read(ORDER_TABLE)
    
    # Join customer and order data
    joined_df = (
        order_df
        .join(customer_df, "CustId", "inner")
        .select(
            customer_df["CustId"],
            customer_df["Name"],
            customer_df["EmailId"],
            customer_df["Region"],
            order_df["OrderId"],
            order_df["ItemName"],
            order_df["PricePerUnit"],
            order_df["Qty"],
            order_df["Date"],
            order_df["TotalAmount"]
        )
    )
    
    # If no existing data, initialize with all records as active
    if not has_existing_data:
        current_timestamp = F.current_timestamp()
        return (
            joined_df
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", current_timestamp)
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
    else:
        # Implement SCD Type 2 logic
        current_timestamp = F.current_timestamp()
        
        # Identify new and changed records
        existing_customer_attrs = existing_data.filter(F.col("IsActive") == True).select(
            "CustId", "Name", "EmailId", "Region"
        ).distinct()
        
        current_customer_attrs = customer_df.select(
            "CustId", "Name", "EmailId", "Region"
        ).distinct()
        
        # Find changed customer records
        changed_customers = (
            current_customer_attrs
            .join(existing_customer_attrs, "CustId", "inner")
            .where(
                (current_customer_attrs["Name"] != existing_customer_attrs["Name"]) |
                (current_customer_attrs["EmailId"] != existing_customer_attrs["EmailId"]) |
                (current_customer_attrs["Region"] != existing_customer_attrs["Region"])
            )
            .select(current_customer_attrs["CustId"])
            .distinct()
        )
        
        # Expire old records
        records_to_expire = (
            existing_data
            .join(changed_customers, "CustId", "inner")
            .where("IsActive = true")
            .withColumn("IsActive", F.lit(False))
            .withColumn("EndDate", current_timestamp)
        )
        
        # Create new active records for changed customers
        new_active_records = (
            joined_df
            .join(changed_customers, "CustId", "inner")
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", current_timestamp)
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
        
        # Find completely new customers
        new_customers = (
            current_customer_attrs
            .join(existing_customer_attrs, "CustId", "left_anti")
            .select("CustId")
        )
        
        # Create records for new customers
        new_customer_records = (
            joined_df
            .join(new_customers, "CustId", "inner")
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", current_timestamp)
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
        
        # Combine all records
        unchanged_records = (
            existing_data
            .join(changed_customers, "CustId", "left_anti")
        )
        
        return (
            unchanged_records
            .unionByName(records_to_expire)
            .unionByName(new_active_records)
            .unionByName(new_customer_records)
        )

# Step 9 & 10: Create customer aggregate spend table
@dlt.table(
    name=CUSTOMER_AGGREGATE_SPEND_TABLE,
    comment="Aggregated customer spending by name and date"
)
def customer_aggregate_spend():
    # Read from the order summary table, only active records
    order_summary_df = dlt.read(ORDER_SUMMARY_TABLE).filter("IsActive = true")
    
    # Aggregate total amount by customer name and date
    return (
        order_summary_df
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )