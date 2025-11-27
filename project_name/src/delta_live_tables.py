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

@dlt.table(
    name="order_silver",
    comment="Cleaned order data with total amount calculated"
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

@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table joining customer and order data",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
def ordersummary():
    # Check if the table exists
    try:
        # Read current state of ordersummary table if it exists
        current_data = dlt.read_stream("ordersummary")
        exists = True
    except:
        exists = False
    
    # Join customer and order data
    joined_data = (
        dlt.read("customer_silver")
        .join(
            dlt.read("order_silver"),
            "CustId",
            "inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date"
        )
    )
    
    if not exists:
        # First load - add SCD Type 2 columns
        return (
            joined_data
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
        )
    else:
        # Implement SCD Type 2 logic for updates
        # This is a simplified version - in production, you would need more complex merge logic
        
        # Get changes from customer table
        customer_changes = dlt.read("customer_silver")
        
        # Identify records to update
        records_to_update = (
            current_data
            .join(
                customer_changes,
                "CustId",
                "inner"
            )
            .filter(
                (current_data["Name"] != customer_changes["Name"]) |
                (current_data["EmailId"] != customer_changes["EmailId"]) |
                (current_data["Region"] != customer_changes["Region"])
            )
            .select(current_data["*"])
        )
        
        # Expire old records
        expired_records = (
            records_to_update
            .withColumn("IsActive", F.lit(False))
            .withColumn("EndDate", F.current_timestamp())
        )
        
        # Create new active records
        new_records = (
            joined_data
            .join(
                records_to_update.select("CustId").distinct(),
                "CustId",
                "inner"
            )
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
        )
        
        # Combine unchanged, expired, and new records
        return (
            current_data
            .filter(~F.col("CustId").isin([r["CustId"] for r in records_to_update.collect()]))
            .union(expired_records)
            .union(new_records)
        )

@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .join(
            dlt.read("order_silver").select("OrderId", "TotalAmount"),
            "OrderId",
            "inner"
        )
        .filter(F.col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )