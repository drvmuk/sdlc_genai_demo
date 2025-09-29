from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
import dlt

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

# Step 1: Read source CSV data from volumes
@dlt.table(
    name="customer_bronze",
    comment="Raw customer data loaded from CSV"
)
def customer_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(customer_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    )

@dlt.table(
    name="order_bronze",
    comment="Raw order data loaded from CSV"
)
def order_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

# Step 3 & 4: Clean data and add TotalAmount column to orders
@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data with no nulls or duplicates"
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
        .dropDuplicates(["OrderId"])
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    )

# Step 5 & 6: Create ordersummary as SCD Type 2
@dlt.table(
    name="ordersummary",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    comment="SCD Type 2 table joining customer and order data",
    spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"},
    temporary=False,
    partition_cols=["Date"]
)
def ordersummary():
    # Check if table exists
    try:
        # Read existing data if available
        existing_data = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        table_exists = True
    except:
        table_exists = False
    
    # Join customer and order data
    joined_data = (
        dlt.read("customer_silver")
        .join(
            dlt.read("order_silver"),
            on="CustId",
            how="inner"
        )
        .select(
            "CustId", 
            "Name", 
            "EmailId", 
            "Region", 
            "OrderId", 
            "ItemName", 
            "PricePerUnit", 
            "Qty", 
            "Date", 
            "TotalAmount"
        )
    )
    
    # For initial load or if table doesn't exist
    if not table_exists:
        return (
            joined_data
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
    
    # For SCD Type 2 updates
    # This will be handled in the autoloader pattern below
    return joined_data

# Step 8: Update SCD Type 2 logic
@dlt.table(
    name="ordersummary_updates",
    comment="Handles SCD Type 2 updates for ordersummary table"
)
def ordersummary_updates():
    # Get current data
    current_data = dlt.read("ordersummary")
    
    # Get new data from the join
    new_data = (
        dlt.read("customer_silver")
        .join(
            dlt.read("order_silver"),
            on="CustId",
            how="inner"
        )
        .select(
            "CustId", 
            "Name", 
            "EmailId", 
            "Region", 
            "OrderId", 
            "ItemName", 
            "PricePerUnit", 
            "Qty", 
            "Date", 
            "TotalAmount"
        )
    )
    
    # Find changed records
    changed_records = (
        new_data.alias("new")
        .join(
            current_data.alias("current")
            .filter(F.col("IsActive") == True),
            on=["CustId", "OrderId"],
            how="inner"
        )
        .where(
            (F.col("new.Name") != F.col("current.Name")) |
            (F.col("new.EmailId") != F.col("current.EmailId")) |
            (F.col("new.Region") != F.col("current.Region"))
        )
        .select("new.*")
    )
    
    # Find new records
    new_records = (
        new_data.alias("new")
        .join(
            current_data.select("CustId", "OrderId").distinct().alias("current"),
            on=["CustId", "OrderId"],
            how="left_anti"
        )
        .select("new.*")
    )
    
    # Combine changed and new records
    updates = (
        changed_records.union(new_records)
        .withColumn("IsActive", F.lit(True))
        .withColumn("StartDate", F.current_timestamp())
        .withColumn("EndDate", F.lit(None).cast(TimestampType()))
    )
    
    return updates

# Step 9 & 10: Create and populate customeraggregatespend
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .filter(F.col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )

# SCD Type 2 CDC process
@dlt.table(
    name="ordersummary_final",
    comment="Final SCD Type 2 table with history"
)
def apply_scd_type2_changes():
    # This function applies the SCD Type 2 changes to the ordersummary table
    
    # Get updates
    updates = dlt.read("ordersummary_updates")
    
    # If there are no updates, return the current table
    if updates.count() == 0:
        return dlt.read("ordersummary")
    
    # Get current active records
    current_records = dlt.read("ordersummary")
    
    # Expire matching records
    records_to_expire = (
        current_records.alias("current")
        .join(
            updates.alias("updates"),
            on=["CustId", "OrderId"],
            how="inner"
        )
        .filter(F.col("current.IsActive") == True)
        .select(
            "current.CustId",
            "current.OrderId",
            "current.Name",
            "current.EmailId",
            "current.Region",
            "current.ItemName",
            "current.PricePerUnit",
            "current.Qty",
            "current.Date",
            "current.TotalAmount",
            F.lit(False).alias("IsActive"),
            "current.StartDate",
            F.current_timestamp().alias("EndDate")
        )
    )
    
    # Combine expired records, unchanged records, and new records
    unchanged_records = (
        current_records.alias("current")
        .join(
            updates.alias("updates"),
            on=["CustId", "OrderId"],
            how="left_anti"
        )
    )
    
    return unchanged_records.union(records_to_expire).union(updates)