import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Define schemas for the datasets
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

# Define catalog and schema names
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

# Step 1: Read source CSV data from volume
@dlt.table(
    name="bronze_customer",
    comment="Raw customer data from source"
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
    comment="Raw order data from source"
)
def bronze_order():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

# Step 4: Clean data - remove nulls and duplicates
@dlt.table(
    name="silver_customer",
    comment="Cleaned customer data with nulls and duplicates removed"
)
def silver_customer():
    return (
        dlt.read("bronze_customer")
        .dropDuplicates()
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
    )

# Step 3 & 4: Add TotalAmount column and clean data
@dlt.table(
    name="silver_order",
    comment="Cleaned order data with TotalAmount calculated"
)
def silver_order():
    return (
        dlt.read("bronze_order")
        .dropDuplicates()
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    )

# Step 5, 6, 7, 8: Create SCD Type 2 ordersummary table
@dlt.table(
    name="gold_ordersummary",
    comment="SCD Type 2 table joining customer and order data",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_custid", "CustId IS NOT NULL")
@dlt.expect_or_drop("valid_orderid", "OrderId IS NOT NULL")
def gold_ordersummary():
    # Get the current state of the target table if it exists
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    # Get current customer and order data
    customer_df = dlt.read("silver_customer")
    order_df = dlt.read("silver_order")
    
    # Join customer and order data
    joined_df = (
        customer_df.join(
            order_df,
            on="CustId",
            how="inner"
        )
        .select(
            customer_df["CustId"],
            customer_df["Name"],
            customer_df["EmailId"],
            customer_df["Region"],
            order_df["OrderId"],
            order_df["ItemName"],
            order_df["PricePerUnit"],
            order_df["Qty"],
            order_df["Date"]
        )
        .withColumn("IsActive", F.lit(True))
        .withColumn("StartDate", F.current_timestamp())
        .withColumn("EndDate", F.lit(None).cast("timestamp"))
    )
    
    # Check if the target table exists
    try:
        # Get existing data
        existing_df = spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
        
        # Find records that need to be updated (customer details changed)
        changed_records = (
            joined_df.join(
                existing_df.filter(F.col("IsActive") == True),
                on=["CustId", "OrderId"],
                how="inner"
            )
            .filter(
                (joined_df["Name"] != existing_df["Name"]) |
                (joined_df["EmailId"] != existing_df["EmailId"]) |
                (joined_df["Region"] != existing_df["Region"])
            )
            .select(
                existing_df["CustId"],
                existing_df["OrderId"]
            )
            .distinct()
        )
        
        # Update existing records (mark as inactive)
        records_to_update = (
            existing_df.join(
                changed_records,
                on=["CustId", "OrderId"],
                how="inner"
            )
            .filter(F.col("IsActive") == True)
            .withColumn("IsActive", F.lit(False))
            .withColumn("EndDate", F.current_timestamp())
        )
        
        # Combine updated records with new records
        result_df = (
            joined_df
            .unionByName(records_to_update)
            .dropDuplicates(["CustId", "OrderId", "IsActive", "StartDate"])
        )
        
        return result_df
    except:
        # If table doesn't exist, return the joined data
        return joined_df

# Step 9 & 10: Create customer aggregate spend table
@dlt.table(
    name="gold_customeraggregatespend",
    comment="Aggregated customer spending data",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_customeraggregatespend():
    # Read from ordersummary and aggregate
    return (
        dlt.read("gold_ordersummary")
        .filter(F.col("IsActive") == True)  # Only consider active records
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )