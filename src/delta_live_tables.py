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

# Step 1 & 2: Read source CSV data and define schemas
@dlt.table(
    name="customer",
    comment="Raw customer data from CSV"
)
def customer():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(customer_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    )

@dlt.table(
    name="order",
    comment="Raw order data from CSV"
)
def order():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(order_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

# Step 3 & 4: Clean data and add TotalAmount column
@dlt.table(
    name="cleaned_customer",
    comment="Cleaned customer data with nulls and duplicates removed"
)
def cleaned_customer():
    return (
        dlt.read("customer")
        .dropDuplicates(["CustId"])
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull()) &
            (F.col("CustId") != "Null") &
            (F.col("Name") != "Null") &
            (F.col("EmailId") != "Null") &
            (F.col("Region") != "Null")
        )
    )

@dlt.table(
    name="cleaned_order",
    comment="Cleaned order data with TotalAmount column added"
)
def cleaned_order():
    return (
        dlt.read("order")
        .dropDuplicates(["OrderId"])
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull()) &
            (F.col("OrderId") != "Null") &
            (F.col("ItemName") != "Null") &
            (F.col("CustId") != "Null")
        )
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    )

# Step 5 & 6 & 7 & 8: Create and update SCD Type 2 table
@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table with customer and order data",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_all_or_drop({"valid_custid": "CustId IS NOT NULL"})
def ordersummary():
    # First time creation logic
    if not DeltaTable.isDeltaTable(spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"):
        # Join customer and order data
        joined_data = (
            dlt.read("cleaned_customer")
            .join(
                dlt.read("cleaned_order"),
                "CustId",
                "inner"
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
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
        return joined_data
    else:
        # SCD Type 2 update logic
        customer_df = dlt.read("cleaned_customer")
        order_df = dlt.read("cleaned_order")
        
        # Get current state of the table
        current_table = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
        # Create new joined dataset
        new_data = (
            customer_df
            .join(
                order_df,
                "CustId",
                "inner"
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
        
        # Find changed records (comparing customer attributes)
        active_records = current_table.filter(F.col("IsActive") == True)
        
        changed_records = (
            new_data
            .join(
                active_records,
                ["CustId", "OrderId"],
                "inner"
            )
            .filter(
                (new_data.Name != active_records.Name) |
                (new_data.EmailId != active_records.EmailId) |
                (new_data.Region != active_records.Region)
            )
            .select(active_records.CustId, active_records.OrderId)
        )
        
        # Records to expire
        records_to_expire = (
            active_records
            .join(
                changed_records,
                ["CustId", "OrderId"],
                "inner"
            )
        )
        
        # New records to insert
        records_to_insert = (
            new_data
            .join(
                changed_records,
                ["CustId", "OrderId"],
                "inner"
            )
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
        
        # Records that don't exist in current active records
        new_records = (
            new_data
            .join(
                active_records.select("CustId", "OrderId"),
                ["CustId", "OrderId"],
                "left_anti"
            )
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
        
        # Combine all records
        result = (
            current_table
            .filter(~F.col("CustId").isin([r.CustId for r in changed_records.collect()]) | 
                   ~F.col("OrderId").isin([r.OrderId for r in changed_records.collect()]))
            .union(
                records_to_expire
                .withColumn("IsActive", F.lit(False))
                .withColumn("EndDate", F.current_timestamp())
            )
            .union(records_to_insert)
            .union(new_records)
        )
        
        return result

# Step 9 & 10: Create customeraggregatespend table
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