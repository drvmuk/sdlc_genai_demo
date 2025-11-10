"""
Delta Live Tables pipeline for customer order processing.
This module implements a complete data pipeline to process customer and order data,
implementing SCD Type 2 for historical tracking and aggregating customer spending.
"""

import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, sum as sum_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

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

# Configuration
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"
CUSTOMER_SOURCE_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_SOURCE_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Step 1: Read source CSV data
@dlt.table(
    name="bronze_customer",
    comment="Raw customer data from CSV files"
)
def bronze_customer():
    return (
        spark.read
        .option("header", "true")
        .schema(customer_schema)
        .csv(CUSTOMER_SOURCE_PATH)
    )

@dlt.table(
    name="bronze_order",
    comment="Raw order data from CSV files"
)
def bronze_order():
    return (
        spark.read
        .option("header", "true")
        .schema(order_schema)
        .csv(ORDER_SOURCE_PATH)
    )

# Step 2-4: Clean data and add TotalAmount column to orders
@dlt.table(
    name="silver_customer",
    comment="Cleaned customer data with nulls and duplicates removed"
)
def silver_customer():
    return (
        dlt.read("bronze_customer")
        .filter(
            col("CustId").isNotNull() &
            col("Name").isNotNull() &
            col("EmailId").isNotNull() &
            col("Region").isNotNull()
        )
        .dropDuplicates(["CustId"])
    )

@dlt.table(
    name="silver_order",
    comment="Cleaned order data with TotalAmount calculated"
)
def silver_order():
    return (
        dlt.read("bronze_order")
        .filter(
            col("OrderId").isNotNull() &
            col("ItemName").isNotNull() &
            col("PricePerUnit").isNotNull() &
            col("Qty").isNotNull() &
            col("Date").isNotNull() &
            col("CustId").isNotNull()
        )
        .dropDuplicates(["OrderId"])
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    )

# Step 5-8: Create SCD Type 2 ordersummary table
@dlt.table(
    name="ordersummary",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    },
    comment="SCD Type 2 table tracking customer and order data history",
    temporary=False,
    spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"}
)
def ordersummary():
    # Get current data by joining customer and order
    current_data = (
        dlt.read("silver_customer")
        .join(
            dlt.read("silver_order"),
            "CustId",
            "inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", 
            "OrderId", "ItemName", "PricePerUnit", "Qty", "Date"
        )
    )
    
    # Check if table exists already
    try:
        # Read existing data
        existing_data = dlt.read("ordersummary")
        
        # Identify new and changed records
        # For SCD Type 2, we need to:
        # 1. Find records that exist in current_data but not in existing_data (new records)
        # 2. Find records that exist in both but have changed (updated records)
        
        # Add SCD Type 2 tracking columns to new data
        new_data = (
            current_data
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )
        
        # Return the result with SCD Type 2 columns
        return new_data
        
    except Exception as e:
        # Table doesn't exist yet, create initial version with SCD Type 2 columns
        return (
            current_data
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )

# Step 9-10: Create customer aggregate spend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date",
    temporary=False
)
def customeraggregatespend():
    return (
        dlt.read("silver_order")
        .join(dlt.read("silver_customer"), "CustId", "inner")
        .groupBy("Name", "Date")
        .agg(sum_("TotalAmount").alias("TotalAmount"))
    )

# SCD Type 2 update logic for ordersummary
@dlt.table(
    name="ordersummary_updates",
    comment="Updates for SCD Type 2 implementation",
    temporary=True
)
def ordersummary_updates():
    # Get current customer and order data
    current_data = (
        dlt.read("silver_customer")
        .join(
            dlt.read("silver_order"),
            "CustId",
            "inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", 
            "OrderId", "ItemName", "PricePerUnit", "Qty", "Date"
        )
    )
    
    # Get existing active records
    existing_active = (
        dlt.read("ordersummary")
        .filter(col("IsActive") == True)
    )
    
    # Find changed records (comparing all fields except tracking columns)
    join_condition = (
        (existing_active["CustId"] == current_data["CustId"]) &
        (existing_active["OrderId"] == current_data["OrderId"])
    )
    
    # Records to expire (changed records that exist in both datasets)
    records_to_expire = (
        existing_active
        .join(
            current_data,
            join_condition,
            "inner"
        )
        .filter(
            (existing_active["Name"] != current_data["Name"]) |
            (existing_active["EmailId"] != current_data["EmailId"]) |
            (existing_active["Region"] != current_data["Region"])
        )
        .select(
            existing_active["CustId"],
            existing_active["OrderId"]
        )
    )
    
    # Records to insert (new versions of changed records)
    records_to_insert = (
        current_data
        .join(
            records_to_expire,
            ["CustId", "OrderId"],
            "inner"
        )
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None).cast("timestamp"))
    )
    
    # New records (don't exist in the target)
    new_records = (
        current_data
        .join(
            existing_active.select("CustId", "OrderId"),
            ["CustId", "OrderId"],
            "left_anti"
        )
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None).cast("timestamp"))
    )
    
    # Combine new records and changed records
    return records_to_insert.union(new_records)

# Apply SCD Type 2 updates
@dlt.table(
    name="apply_ordersummary_updates",
    comment="Apply SCD Type 2 updates to ordersummary table",
    temporary=True
)
def apply_ordersummary_updates():
    # Get records to expire
    existing_active = (
        dlt.read("ordersummary")
        .filter(col("IsActive") == True)
    )
    
    updates = dlt.read("ordersummary_updates")
    
    # Find records to expire
    records_to_expire = (
        existing_active
        .join(
            updates.select("CustId", "OrderId"),
            ["CustId", "OrderId"],
            "inner"
        )
    )
    
    # Update existing records (set IsActive=False and EndDate=current_timestamp)
    expired_records = (
        records_to_expire
        .withColumn("IsActive", lit(False))
        .withColumn("EndDate", current_timestamp())
    )
    
    # Combine expired records with new records
    return expired_records.union(updates)