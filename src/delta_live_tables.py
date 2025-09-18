from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import dlt

# Define source data paths
CUSTOMER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Define catalog and schema
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

# Step 1 & 2: Read source data and define schema
@dlt.table(
    name="customer_bronze",
    comment="Raw customer data loaded from CSV"
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
    comment="Raw order data loaded from CSV"
)
def order_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(ORDER_PATH)
    )

# Step 3 & 4: Clean data and add TotalAmount column
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
        .dropDuplicates()
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
        .dropDuplicates()
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    )

# Step 5, 6, 7, 8: Create and update SCD Type 2 ordersummary table
@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table combining customer and order data",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_all_or_drop({"valid_custid": "CustId IS NOT NULL"})
def ordersummary():
    # Get current data
    current_customer = dlt.read("customer_silver")
    current_order = dlt.read("order_silver")
    
    # Join customer and order data
    current_data = (
        current_customer.join(
            current_order,
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
    
    # For first-time load, add SCD Type 2 columns
    if not dlt.table_exists("ordersummary"):
        return (
            current_data
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
            .withColumn("IsActive", F.lit(True))
        )
    else:
        # Get existing data
        existing_data = dlt.read("ordersummary")
        
        # Identify records that need to be updated (where customer info has changed)
        # First, get the latest version of each customer record
        window_spec = Window.partitionBy("CustId").orderBy(F.desc("StartDate"))
        latest_customer_records = (
            existing_data
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .filter(F.col("IsActive") == True)
            .drop("row_num")
        )
        
        # Find changed records
        changed_records = (
            current_data.join(
                latest_customer_records,
                on=["CustId"],
                how="left_anti"
            )
        )
        
        # Expire old records
        expired_records = (
            latest_customer_records
            .join(
                changed_records.select("CustId").distinct(),
                on=["CustId"],
                how="inner"
            )
            .withColumn("EndDate", F.current_timestamp())
            .withColumn("IsActive", F.lit(False))
        )
        
        # Create new active records
        new_records = (
            changed_records
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
            .withColumn("IsActive", F.lit(True))
        )
        
        # Combine unchanged, expired, and new records
        unchanged_records = (
            existing_data.join(
                expired_records.select("CustId"),
                on=["CustId"],
                how="left_anti"
            )
        )
        
        return (
            unchanged_records
            .unionByName(expired_records)
            .unionByName(new_records)
        )

# Step 9 & 10: Create and populate customeraggregatespend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .filter(F.col("IsActive") == True)  # Use only active records
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )