import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, lag, max as max_
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

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

# Source tables
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

# Silver tables - cleaned data
@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data with no nulls or duplicates"
)
def customer_silver():
    return (
        dlt.read("customer_bronze")
        .filter(
            (col("CustId").isNotNull()) &
            (col("Name").isNotNull()) &
            (col("EmailId").isNotNull()) &
            (col("Region").isNotNull())
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
            (col("OrderId").isNotNull()) &
            (col("ItemName").isNotNull()) &
            (col("PricePerUnit").isNotNull()) &
            (col("Qty").isNotNull()) &
            (col("Date").isNotNull()) &
            (col("CustId").isNotNull())
        )
        .dropDuplicates(["OrderId"])
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    )

# Gold tables - business value
@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table combining customer and order data",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_fail("valid_custid", "CustId IS NOT NULL")
def ordersummary():
    # Get current state of the table if it exists
    try:
        existing_data = dlt.read("ordersummary")
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
    
    # For initial load
    if not exists:
        return (
            joined_data
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast(TimestampType()))
            .withColumn("ChangeHash", expr("md5(concat(Name, EmailId, Region))"))
        )
    
    # For updates (SCD Type 2 implementation)
    # Identify changes in customer data
    customer_changes = (
        dlt.read("customer_silver")
        .withColumn("ChangeHash", expr("md5(concat(Name, EmailId, Region))"))
        .alias("new")
        .join(
            existing_data.select("CustId", "ChangeHash").alias("old"),
            col("new.CustId") == col("old.CustId"),
            "left"
        )
        .where(
            (col("new.ChangeHash") != col("old.ChangeHash")) | 
            col("old.ChangeHash").isNull()
        )
        .select(
            col("new.CustId").alias("CustId"),
            col("new.ChangeHash").alias("NewHash")
        )
    )
    
    # Mark existing records as inactive
    updated_existing = (
        existing_data
        .join(customer_changes, "CustId", "left")
        .withColumn(
            "IsActive", 
            when(col("NewHash").isNotNull(), False).otherwise(col("IsActive"))
        )
        .withColumn(
            "EndDate", 
            when(col("NewHash").isNotNull(), current_timestamp()).otherwise(col("EndDate"))
        )
        .drop("NewHash")
    )
    
    # Create new active records
    new_records = (
        joined_data
        .join(customer_changes, "CustId", "inner")
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None).cast(TimestampType()))
        .withColumn("ChangeHash", expr("md5(concat(Name, EmailId, Region))"))
        .drop("NewHash")
    )
    
    # Union existing and new records
    return updated_existing.unionByName(new_records, allowMissingColumns=True)

@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .where("IsActive = true")
        .groupBy("Name", "Date")
        .agg(expr("sum(TotalAmount)").alias("TotalAmount"))
    )