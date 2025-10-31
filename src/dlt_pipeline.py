import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Define schemas for customer and order data
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

# Step 1: Read source CSV data from volume
@dlt.table(
    name="customer_bronze",
    comment="Raw customer data from CSV files"
)
def customer_bronze():
    return (
        spark.read
        .option("header", "true")
        .schema(customer_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    )

@dlt.table(
    name="order_bronze",
    comment="Raw order data from CSV files"
)
def order_bronze():
    return (
        spark.read
        .option("header", "true")
        .schema(order_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

# Step 4: Clean data - remove nulls and duplicates
@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data with nulls and duplicates removed"
)
def customer_silver():
    return (
        dlt.read("customer_bronze")
        .filter(
            (F.col("CustId").isNotNull()) & 
            (F.col("Name").isNotNull()) & 
            (F.col("EmailId").isNotNull()) & 
            (F.col("Region").isNotNull())
        )
        .dropDuplicates(["CustId"])
    )

# Step 3 & 4: Add TotalAmount column, remove nulls and duplicates
@dlt.table(
    name="order_silver",
    comment="Cleaned order data with TotalAmount calculated"
)
def order_silver():
    return (
        dlt.read("order_bronze")
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

# Step 6, 7, 8: Create SCD Type 2 table for order summary
@dlt.table(
    name="ordersummary",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    },
    comment="SCD Type 2 table joining customer and order data",
    temporary=False,
    spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"}
)
def ordersummary():
    # Get current data
    current_data = (
        dlt.read("customer_silver")
        .join(
            dlt.read("order_silver"),
            "CustId",
            "inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
        )
    )
    
    # Check if the target table exists
    try:
        existing_data = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
        # If table exists, implement SCD Type 2 logic
        # 1. Find records that need to be updated (changed customer info)
        changed_records = (
            current_data
            .join(
                existing_data.filter(F.col("IsActive") == True),
                ["CustId", "OrderId"],
                "inner"
            )
            .filter(
                (current_data["Name"] != existing_data["Name"]) | 
                (current_data["EmailId"] != existing_data["EmailId"]) | 
                (current_data["Region"] != existing_data["Region"])
            )
            .select(existing_data["CustId"], existing_data["OrderId"])
        )
        
        # 2. Expire the old records
        expired_records = (
            existing_data
            .join(changed_records, ["CustId", "OrderId"], "inner")
            .withColumn("IsActive", F.lit(False))
            .withColumn("EndDate", F.current_date())
        )
        
        # 3. Create new active records
        new_active_records = (
            current_data
            .join(changed_records, ["CustId", "OrderId"], "inner")
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_date())
            .withColumn("EndDate", F.lit(None).cast("date"))
        )
        
        # 4. Get unchanged records
        unchanged_records = (
            existing_data
            .join(changed_records, ["CustId", "OrderId"], "leftanti")
        )
        
        # 5. Get completely new records
        new_records = (
            current_data
            .join(
                existing_data.select("CustId", "OrderId").distinct(),
                ["CustId", "OrderId"],
                "leftanti"
            )
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_date())
            .withColumn("EndDate", F.lit(None).cast("date"))
        )
        
        # Union all the record sets
        return (
            unchanged_records
            .unionByName(expired_records)
            .unionByName(new_active_records)
            .unionByName(new_records)
        )
    
    except:
        # If table doesn't exist, create initial version with all records as active
        return (
            current_data
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_date())
            .withColumn("EndDate", F.lit(None).cast("date"))
        )

# Step 9 & 10: Create customer aggregate spend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date",
    temporary=False
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .filter(F.col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )