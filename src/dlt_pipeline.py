"""
Delta Live Tables pipeline for customer order analytics.
This module defines the DLT pipeline that processes customer and order data.
"""

import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Define schemas for the input data
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

# Step 1: Read source CSV data
@dlt.table(
    name="bronze_customer",
    comment="Raw customer data from CSV files"
)
def bronze_customer():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(customer_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    )

@dlt.table(
    name="bronze_order",
    comment="Raw order data from CSV files"
)
def bronze_order():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

# Step 2 & 4: Clean customer data - remove nulls and duplicates
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

# Step 3 & 4: Clean order data - add TotalAmount column and remove nulls/duplicates
@dlt.table(
    name="silver_order",
    comment="Cleaned order data with TotalAmount calculated and nulls/duplicates removed"
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
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        .dropDuplicates(["OrderId"])
    )

# Step 6, 7 & 8: Create SCD Type 2 table for customer and order data
@dlt.table(
    name="ordersummary",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    comment="SCD Type 2 table combining customer and order data",
    spark_conf={"pipelines.autoOptimize.managed": "true"},
    schema="sdlc_wizard",
    catalog="gen_ai_poc_databrickscoe"
)
def ordersummary():
    # Check if the table exists
    try:
        existing_data = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        table_exists = True
    except:
        table_exists = False
    
    # Join customer and order data
    joined_data = (
        dlt.read("silver_customer")
        .join(
            dlt.read("silver_order"),
            on="CustId",
            how="inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
        )
    )
    
    if not table_exists:
        # First time load - add SCD Type 2 columns
        return (
            joined_data
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
            .withColumn("HashKey", expr("sha2(concat_ws('|', CustId, Name, EmailId, Region), 256)"))
        )
    else:
        # Process changes for SCD Type 2
        # Get current records
        current_data = (
            joined_data
            .withColumn("HashKey", expr("sha2(concat_ws('|', CustId, Name, EmailId, Region), 256)"))
        )
        
        # Get existing records
        existing_active = existing_data.filter(col("IsActive") == True)
        
        # Identify changed records
        changed_records = (
            current_data.join(
                existing_active.select("CustId", "HashKey"),
                on="CustId",
                how="inner"
            )
            .filter(current_data["HashKey"] != existing_active["HashKey"])
            .select(current_data["*"])
        )
        
        # Expire old records
        records_to_expire = (
            existing_active
            .join(changed_records.select("CustId"), on="CustId", how="inner")
            .withColumn("IsActive", lit(False))
            .withColumn("EndDate", current_timestamp())
        )
        
        # Create new active records
        new_active_records = (
            changed_records
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )
        
        # Identify completely new records
        new_records = (
            current_data
            .join(existing_active.select("CustId"), on="CustId", how="left_anti")
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )
        
        # Combine unchanged records, expired records, and new records
        unchanged_records = (
            existing_active
            .join(changed_records.select("CustId"), on="CustId", how="left_anti")
        )
        
        # Combine all record types
        return (
            unchanged_records
            .unionByName(records_to_expire)
            .unionByName(new_active_records)
            .unionByName(new_records)
        )

# Step 9 & 10: Create customer aggregate spend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date",
    schema="sdlc_wizard",
    catalog="gen_ai_poc_databrickscoe"
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .filter(col("IsActive") == True)  # Only use active records
        .groupBy("Name", "Date")
        .agg({"TotalAmount": "sum"})
        .withColumnRenamed("sum(TotalAmount)", "TotalAmount")
        .select("Name", "TotalAmount", "Date")
    )