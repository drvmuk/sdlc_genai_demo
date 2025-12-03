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

# Step 1: Read source CSV data
@dlt.table(
    name="customer_bronze",
    comment="Raw customer data from CSV"
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
    comment="Raw order data from CSV"
)
def order_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

# Step 4: Clean data - remove nulls and duplicates
@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data"
)
def customer_silver():
    return (
        dlt.read("customer_bronze")
        .dropDuplicates()
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
    )

# Step 3 & 4: Add TotalAmount column, clean data
@dlt.table(
    name="order_silver",
    comment="Cleaned order data with TotalAmount calculated"
)
def order_silver():
    return (
        dlt.read("order_bronze")
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .dropDuplicates()
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
    )

# Step 5-8: Create SCD Type 2 ordersummary table
@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table combining customer and order data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["Date"],
    schema="sdlc_wizard",
    catalog="gen_ai_poc_databrickscoe"
)
def ordersummary():
    # Get current and previous customer data
    current_customer = dlt.read("customer_silver")
    
    # Join with orders
    joined_data = (
        dlt.read("order_silver")
        .join(
            current_customer,
            on="CustId",
            how="inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date"
        )
    )
    
    # Add SCD Type 2 columns
    result = (
        joined_data
        .withColumn("IsActive", F.lit(True))
        .withColumn("StartDate", F.current_timestamp())
        .withColumn("EndDate", F.lit(None).cast(TimestampType()))
    )
    
    return result

# Function to update SCD Type 2 table
@dlt.table_property(name="pipelines.autoOptimize.managed", value="true")
@dlt.expect_or_drop("valid_custid", "CustId IS NOT NULL")
@dlt.expect_or_drop("valid_name", "Name IS NOT NULL")
@dlt.table(
    name="ordersummary_updates",
    comment="Updates for the SCD Type 2 table",
    temporary=True
)
def ordersummary_updates():
    # This is a temporary table to handle SCD Type 2 updates
    # In a real implementation, we would compare with the previous version
    # and generate appropriate updates
    
    # For demonstration, we'll simulate changes by joining current customer data with orders
    current_customer = dlt.read("customer_silver")
    
    return (
        dlt.read("order_silver")
        .join(
            current_customer,
            on="CustId",
            how="inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date"
        )
        .withColumn("IsActive", F.lit(True))
        .withColumn("StartDate", F.current_timestamp())
        .withColumn("EndDate", F.lit(None).cast(TimestampType()))
    )

# Step 9-10: Create customeraggregatespend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by date",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    schema="sdlc_wizard",
    catalog="gen_ai_poc_databrickscoe"
)
def customeraggregatespend():
    return (
        dlt.read("order_silver")
        .join(
            dlt.read("customer_silver"),
            on="CustId",
            how="inner"
        )
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )