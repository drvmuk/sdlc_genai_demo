import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, sum as sum_
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

# Define catalog and schema names
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

# Step 1: Read source CSV data from volume
@dlt.table(
    name="bronze_customer",
    comment="Raw customer data from CSV"
)
def bronze_customer():
    return (
        spark.read
        .option("header", "true")
        .schema(customer_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    )

@dlt.table(
    name="bronze_order",
    comment="Raw order data from CSV"
)
def bronze_order():
    return (
        spark.read
        .option("header", "true")
        .schema(order_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

# Step 2 & 4: Clean customer data - remove nulls and duplicates
@dlt.table(
    name="silver_customer",
    comment="Cleaned customer data"
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

# Step 3 & 4: Clean order data, add TotalAmount column, remove nulls and duplicates
@dlt.table(
    name="silver_order",
    comment="Cleaned order data with TotalAmount calculated"
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

# Step 5, 6, 7, 8: Create ordersummary table with SCD Type 2
@dlt.table(
    name="ordersummary",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    },
    comment="SCD Type 2 table joining customer and order data",
    temporary=False
)
@dlt.expect_all_or_drop({"valid_custid": "CustId IS NOT NULL"})
def ordersummary():
    # Get current data in the table if it exists
    try:
        current_data = spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
        current_data_exists = True
    except:
        current_data_exists = False
    
    # Get new data by joining customer and order
    new_data = (
        dlt.read("silver_customer")
        .join(
            dlt.read("silver_order"),
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
    
    # If table doesn't exist yet, create it with initial data
    if not current_data_exists:
        return (
            new_data
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )
    
    # Identify changed records
    customer_changes = (
        dlt.read("silver_customer")
        .join(
            current_data.filter(col("IsActive") == True)
            .select("CustId", "Name", "EmailId", "Region"),
            "CustId",
            "inner"
        )
        .filter(
            (col("silver_customer.Name") != col("ordersummary.Name")) |
            (col("silver_customer.EmailId") != col("ordersummary.EmailId")) |
            (col("silver_customer.Region") != col("ordersummary.Region"))
        )
        .select("silver_customer.CustId")
        .distinct()
    )
    
    # Expire old records
    expired_records = (
        current_data
        .join(customer_changes, "CustId", "inner")
        .filter(col("IsActive") == True)
        .withColumn("IsActive", lit(False))
        .withColumn("EndDate", current_timestamp())
    )
    
    # Create new active records
    new_active_records = (
        new_data
        .join(customer_changes, "CustId", "inner")
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None).cast("timestamp"))
    )
    
    # Combine unchanged records, expired records, and new active records
    unchanged_records = (
        current_data
        .join(customer_changes, "CustId", "left_anti")
    )
    
    return (
        unchanged_records
        .union(expired_records)
        .union(new_active_records)
    )

# Step 9 & 10: Create customeraggregatespend table with aggregated data
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by date",
    table_properties={"quality": "gold"}
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .filter(col("IsActive") == True)  # Only use active records
        .groupBy("Name", "Date")
        .agg(sum_("TotalAmount").alias("TotalAmount"))
    )