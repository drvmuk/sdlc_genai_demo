import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import datetime

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
    name="customer",
    comment="Customer data from CSV",
    schema=customer_schema
)
def customer():
    return (
        spark.read
        .option("header", "true")
        .schema(customer_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
        .dropna()  # Remove nulls
        .dropDuplicates()  # Remove duplicates
    )

@dlt.table(
    name="order",
    comment="Order data from CSV with TotalAmount calculated",
    schema=order_schema.add(StructField("TotalAmount", DoubleType(), True))
)
def order():
    return (
        spark.read
        .option("header", "true")
        .schema(order_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))  # Add TotalAmount
        .dropna()  # Remove nulls
        .dropDuplicates()  # Remove duplicates
    )

# SCD Type 2 implementation for ordersummary
@dlt.table(
    name="ordersummary",
    comment="Order summary with customer details using SCD Type 2"
)
@dlt.expect_all_or_drop({"valid_customer_id": "CustId IS NOT NULL", "valid_order_id": "OrderId IS NOT NULL"})
def ordersummary():
    # Get the current data from the table if it exists
    try:
        current_data = dlt.read("ordersummary")
        has_current_data = True
    except:
        has_current_data = False
    
    # Join customer and order data
    joined_data = (
        dlt.read("order")
        .join(
            dlt.read("customer"),
            on="CustId",
            how="inner"
        )
        .select(
            dlt.read("customer")["CustId"],
            dlt.read("customer")["Name"],
            dlt.read("customer")["EmailId"],
            dlt.read("customer")["Region"],
            dlt.read("order")["OrderId"],
            dlt.read("order")["ItemName"],
            dlt.read("order")["PricePerUnit"],
            dlt.read("order")["Qty"],
            dlt.read("order")["Date"],
            dlt.read("order")["TotalAmount"]
        )
    )
    
    if not has_current_data:
        # First run - initialize with active records
        return (
            joined_data
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )
    else:
        # Get active records
        current_active = current_data.filter(col("IsActive") == True)
        
        # Identify records that have changed
        key_columns = ["CustId", "OrderId"]
        change_columns = ["Name", "EmailId", "Region", "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"]
        
        # Join to find matching records
        matched_records = (
            current_active
            .join(
                joined_data,
                on=key_columns,
                how="inner"
            )
        )
        
        # Identify changed records
        change_condition = " OR ".join([f"current.{col_name} <> new.{col_name}" for col_name in change_columns])
        changed_records_expr = expr(change_condition)
        
        # Mark records that need to be expired
        records_to_expire = (
            matched_records
            .filter(changed_records_expr)
            .select(
                current_active["*"],
                lit(False).alias("IsActive_new"),
                current_timestamp().alias("EndDate_new")
            )
        )
        
        # Update expired records
        expired_records = (
            records_to_expire
            .select(
                col("CustId"),
                col("Name"),
                col("EmailId"),
                col("Region"),
                col("OrderId"),
                col("ItemName"),
                col("PricePerUnit"),
                col("Qty"),
                col("Date"),
                col("TotalAmount"),
                col("IsActive_new").alias("IsActive"),
                col("StartDate"),
                col("EndDate_new").alias("EndDate")
            )
        )
        
        # Get records that need to be inserted (changed records with new values)
        new_changed_records = (
            records_to_expire
            .join(
                joined_data,
                on=key_columns,
                how="inner"
            )
            .select(
                joined_data["CustId"],
                joined_data["Name"],
                joined_data["EmailId"],
                joined_data["Region"],
                joined_data["OrderId"],
                joined_data["ItemName"],
                joined_data["PricePerUnit"],
                joined_data["Qty"],
                joined_data["Date"],
                joined_data["TotalAmount"],
                lit(True).alias("IsActive"),
                current_timestamp().alias("StartDate"),
                lit(None).cast("timestamp").alias("EndDate")
            )
        )
        
        # Get completely new records (not in current data)
        completely_new_records = (
            joined_data
            .join(
                current_active.select(*key_columns),
                on=key_columns,
                how="left_anti"
            )
            .select(
                joined_data["*"],
                lit(True).alias("IsActive"),
                current_timestamp().alias("StartDate"),
                lit(None).cast("timestamp").alias("EndDate")
            )
        )
        
        # Get unchanged records
        unchanged_records = (
            current_data
            .join(
                expired_records.select(*key_columns),
                on=key_columns,
                how="left_anti"
            )
        )
        
        # Combine all record sets
        return (
            unchanged_records
            .unionByName(expired_records)
            .unionByName(new_changed_records)
            .unionByName(completely_new_records)
        )

# Customer aggregate spend
@dlt.table(
    name="customeraggregatespend",
    comment="Customer aggregate spend by name and date"
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .filter(col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg({"TotalAmount": "sum"})
        .select(
            col("Name"),
            col("sum(TotalAmount)").alias("TotalAmount"),
            col("Date")
        )
    )