import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when, lead, expr, sum as sum_
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

# Define the catalog and schema names
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

# Define schemas for source data
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
    name="customer",
    comment="Raw customer data loaded from CSV"
)
def customer():
    return (
        spark.read.schema(customer_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
        .filter(col("CustId").isNotNull() & 
                col("Name").isNotNull() & 
                col("EmailId").isNotNull() & 
                col("Region").isNotNull())
        .dropDuplicates()
    )

@dlt.table(
    name="order",
    comment="Raw order data loaded from CSV with TotalAmount calculated"
)
def order():
    return (
        spark.read.schema(order_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
        .filter(col("OrderId").isNotNull() & 
                col("ItemName").isNotNull() & 
                col("PricePerUnit").isNotNull() & 
                col("Qty").isNotNull() & 
                col("Date").isNotNull() & 
                col("CustId").isNotNull())
        .dropDuplicates()
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    )

# SCD Type 2 implementation for ordersummary table
@dlt.table(
    name="ordersummary",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    comment="SCD Type 2 table containing customer and order data",
    temporary=False,
    schema=StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("TotalAmount", DoubleType(), True),
        StructField("IsActive", BooleanType(), True),
        StructField("StartDate", TimestampType(), True),
        StructField("EndDate", TimestampType(), True)
    ])
)
def ordersummary():
    # Get the current and previous customer data
    current_customer = dlt.read("customer")
    
    # Get the order data
    orders = dlt.read("order")
    
    # Join current customer and order data
    current_joined = (
        current_customer.join(
            orders,
            on="CustId",
            how="inner"
        )
    )
    
    # Try to read existing ordersummary data
    try:
        existing_data = spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
        
        # Identify records that need to be updated (where customer data has changed)
        window_spec = Window.partitionBy("CustId", "OrderId").orderBy("StartDate")
        
        # Get active records from existing data
        active_records = existing_data.filter(col("IsActive") == True)
        
        # Find records that have changed in the customer data
        changed_records = (
            active_records.join(
                current_joined,
                on=["CustId", "OrderId"],
                how="inner"
            )
            .filter(
                (active_records["Name"] != current_joined["Name"]) |
                (active_records["EmailId"] != current_joined["EmailId"]) |
                (active_records["Region"] != current_joined["Region"])
            )
            .select(active_records["CustId"], active_records["OrderId"])
            .distinct()
        )
        
        # Mark existing active records as inactive if they have changed
        records_to_expire = (
            active_records.join(
                changed_records,
                on=["CustId", "OrderId"],
                how="inner"
            )
            .withColumn("IsActive", lit(False))
            .withColumn("EndDate", current_timestamp())
        )
        
        # Create new active records for the changed data
        new_active_records = (
            current_joined.join(
                changed_records,
                on=["CustId", "OrderId"],
                how="inner"
            )
            .select(
                current_joined["CustId"],
                current_joined["Name"],
                current_joined["EmailId"],
                current_joined["Region"],
                current_joined["OrderId"],
                current_joined["ItemName"],
                current_joined["PricePerUnit"],
                current_joined["Qty"],
                current_joined["Date"],
                current_joined["TotalAmount"],
                lit(True).alias("IsActive"),
                current_timestamp().alias("StartDate"),
                lit(None).cast(TimestampType()).alias("EndDate")
            )
        )
        
        # Find completely new records (not in existing data)
        existing_keys = active_records.select("CustId", "OrderId").distinct()
        new_records = (
            current_joined.join(
                existing_keys,
                on=["CustId", "OrderId"],
                how="left_anti"
            )
            .select(
                current_joined["CustId"],
                current_joined["Name"],
                current_joined["EmailId"],
                current_joined["Region"],
                current_joined["OrderId"],
                current_joined["ItemName"],
                current_joined["PricePerUnit"],
                current_joined["Qty"],
                current_joined["Date"],
                current_joined["TotalAmount"],
                lit(True).alias("IsActive"),
                current_timestamp().alias("StartDate"),
                lit(None).cast(TimestampType()).alias("EndDate")
            )
        )
        
        # Combine unchanged records, expired records, new active records, and completely new records
        unchanged_records = active_records.join(
            changed_records,
            on=["CustId", "OrderId"],
            how="left_anti"
        )
        
        inactive_records = existing_data.filter(col("IsActive") == False)
        
        return (
            unchanged_records
            .unionByName(inactive_records)
            .unionByName(records_to_expire)
            .unionByName(new_active_records)
            .unionByName(new_records)
        )
        
    except Exception as e:
        # If the table doesn't exist yet or there's an error, create initial data
        return (
            current_joined
            .select(
                current_joined["CustId"],
                current_joined["Name"],
                current_joined["EmailId"],
                current_joined["Region"],
                current_joined["OrderId"],
                current_joined["ItemName"],
                current_joined["PricePerUnit"],
                current_joined["Qty"],
                current_joined["Date"],
                current_joined["TotalAmount"],
                lit(True).alias("IsActive"),
                current_timestamp().alias("StartDate"),
                lit(None).cast(TimestampType()).alias("EndDate")
            )
        )

# Create customer aggregate spend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date",
    temporary=False
)
def customeraggregatespend():
    # Read from the ordersummary table
    ordersummary_data = dlt.read("ordersummary")
    
    # Only use active records for aggregation
    active_records = ordersummary_data.filter(col("IsActive") == True)
    
    # Aggregate TotalAmount by Name and Date
    return (
        active_records
        .groupBy("Name", "Date")
        .agg(sum_("TotalAmount").alias("TotalAmount"))
    )