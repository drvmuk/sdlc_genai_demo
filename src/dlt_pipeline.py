# Delta Live Tables implementation for Customer Order Analytics Pipeline

import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, sum as sum_

# Define the catalog and schema
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

# Define source paths
CUSTOMER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Bronze layer: Raw data ingestion
@dlt.table(
    name="customer_bronze",
    comment="Raw customer data from source"
)
def customer_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(CUSTOMER_PATH)
    )

@dlt.table(
    name="order_bronze",
    comment="Raw order data from source"
)
def order_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(ORDER_PATH)
    )

# Silver layer: Cleaned data
@dlt.table(
    name="customer",
    comment="Cleaned customer data"
)
def customer():
    # Remove nulls and duplicates
    return (
        dlt.read("customer_bronze")
        .dropDuplicates(["CustId"])
        .filter(
            (col("CustId").isNotNull()) &
            (col("Name").isNotNull()) &
            (col("EmailId").isNotNull()) &
            (col("Region").isNotNull())
        )
    )

@dlt.table(
    name="order",
    comment="Cleaned order data with TotalAmount calculated"
)
def order():
    # Remove nulls and duplicates, add TotalAmount column
    return (
        dlt.read("order_bronze")
        .dropDuplicates(["OrderId"])
        .filter(
            (col("OrderId").isNotNull()) &
            (col("ItemName").isNotNull()) &
            (col("PricePerUnit").isNotNull()) &
            (col("Qty").isNotNull()) &
            (col("Date").isNotNull()) &
            (col("CustId").isNotNull())
        )
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    )

# Gold layer: SCD Type 2 implementation
@dlt.table(
    name="ordersummary",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    },
    comment="SCD Type 2 table tracking customer and order history"
)
def ordersummary():
    # Get current data by joining customer and order
    current_data = (
        dlt.read("customer")
        .join(
            dlt.read("order"),
            "CustId",
            "inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", 
            "OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
        )
    )
    
    # Check if table exists
    try:
        # If table exists, implement SCD Type 2 logic
        existing_data = spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
        
        # Get active records
        active_records = existing_data.filter(col("IsActive") == True)
        
        # Find changed records
        changed_records = (
            current_data
            .join(
                active_records,
                ["CustId", "OrderId"],
                "inner"
            )
            .filter(
                (col("current_data.Name") != col("active_records.Name")) |
                (col("current_data.EmailId") != col("active_records.EmailId")) |
                (col("current_data.Region") != col("active_records.Region"))
            )
            .select(
                active_records["CustId"],
                active_records["OrderId"]
            )
        )
        
        # Expire old records
        expired_records = (
            active_records
            .join(
                changed_records,
                ["CustId", "OrderId"],
                "inner"
            )
            .withColumn("IsActive", lit(False))
            .withColumn("EndDate", current_timestamp())
        )
        
        # Create new active records
        new_active_records = (
            current_data
            .join(
                changed_records,
                ["CustId", "OrderId"],
                "inner"
            )
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )
        
        # Find completely new records
        new_records = (
            current_data
            .join(
                active_records.select("CustId", "OrderId"),
                ["CustId", "OrderId"],
                "left_anti"
            )
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )
        
        # Combine unchanged active records, expired records, new active versions, and new records
        unchanged_active = (
            active_records
            .join(
                changed_records,
                ["CustId", "OrderId"],
                "left_anti"
            )
        )
        
        return (
            unchanged_active
            .unionByName(expired_records)
            .unionByName(new_active_records)
            .unionByName(new_records)
        )
        
    except:
        # If table doesn't exist, create initial version with all records active
        return (
            current_data
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )

# Aggregate spending by customer and date
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by date"
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .filter(col("IsActive") == True)  # Only consider active records
        .groupBy("Name", "Date")
        .agg(sum_("TotalAmount").alias("TotalAmount"))
    )