import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when, expr
from pyspark.sql.types import TimestampType

# Define the catalog and schema
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

# Define source paths
CUSTOMER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Step 1: Read source data
@dlt.table(
    name="customer_bronze",
    comment="Raw customer data loaded from CSV"
)
def customer_bronze():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(CUSTOMER_PATH)

@dlt.table(
    name="order_bronze",
    comment="Raw order data loaded from CSV"
)
def order_bronze():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(ORDER_PATH)

# Step 2 & 4: Clean data and remove nulls/duplicates
@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data with nulls and duplicates removed"
)
def customer_silver():
    return dlt.read("customer_bronze") \
        .filter(
            col("CustId").isNotNull() & 
            col("Name").isNotNull() & 
            col("EmailId").isNotNull() & 
            col("Region").isNotNull()
        ).dropDuplicates()

# Step 3 & 4: Add TotalAmount column and clean order data
@dlt.table(
    name="order_silver",
    comment="Cleaned order data with TotalAmount calculated and nulls/duplicates removed"
)
def order_silver():
    return dlt.read("order_bronze") \
        .filter(
            col("OrderId").isNotNull() & 
            col("ItemName").isNotNull() & 
            col("PricePerUnit").isNotNull() & 
            col("Qty").isNotNull() & 
            col("Date").isNotNull() & 
            col("CustId").isNotNull()
        ).dropDuplicates() \
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))

# Step 6-8: Create SCD Type 2 table by joining customer and order data
@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table with customer and order data",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_fail("valid_customer_id", "CustId IS NOT NULL")
def ordersummary():
    # Get the current data from the silver tables
    customer_df = dlt.read("customer_silver")
    order_df = dlt.read("order_silver")
    
    # Join customer and order data
    joined_df = customer_df.join(order_df, "CustId", "inner").select(
        customer_df["CustId"],
        customer_df["Name"],
        customer_df["EmailId"],
        customer_df["Region"],
        order_df["OrderId"],
        order_df["ItemName"],
        order_df["PricePerUnit"],
        order_df["Qty"],
        order_df["Date"],
        order_df["TotalAmount"]
    )
    
    # Check if the table exists and has data
    try:
        existing_data = spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
        # If we get here, the table exists
        
        # Generate a unique key for matching
        joined_df = joined_df.withColumn("mergeKey", expr("CONCAT(CustId, '|', OrderId)"))
        existing_data = existing_data.withColumn("mergeKey", expr("CONCAT(CustId, '|', OrderId)"))
        
        # Find records that need to be updated (changed)
        changed_records = joined_df.join(
            existing_data,
            (joined_df["mergeKey"] == existing_data["mergeKey"]) & 
            (existing_data["IsActive"] == True),
            "inner"
        ).filter(
            (joined_df["Name"] != existing_data["Name"]) |
            (joined_df["EmailId"] != existing_data["EmailId"]) |
            (joined_df["Region"] != existing_data["Region"]) |
            (joined_df["ItemName"] != existing_data["ItemName"]) |
            (joined_df["PricePerUnit"] != existing_data["PricePerUnit"]) |
            (joined_df["Qty"] != existing_data["Qty"]) |
            (joined_df["Date"] != existing_data["Date"]) |
            (joined_df["TotalAmount"] != existing_data["TotalAmount"])
        ).select(existing_data["*"])
        
        # Mark changed records as inactive
        updated_existing = existing_data.join(
            changed_records,
            "mergeKey",
            "left_outer"
        ).withColumn(
            "IsActive",
            when(changed_records["CustId"].isNotNull(), False).otherwise(existing_data["IsActive"])
        ).withColumn(
            "EndDate",
            when(changed_records["CustId"].isNotNull(), current_timestamp()).otherwise(existing_data["EndDate"])
        )
        
        # Create new active records for the changed data
        new_active_records = joined_df.join(
            changed_records.select("mergeKey"),
            "mergeKey",
            "inner"
        ).select(
            joined_df["CustId"],
            joined_df["Name"],
            joined_df["EmailId"],
            joined_df["Region"],
            joined_df["OrderId"],
            joined_df["ItemName"],
            joined_df["PricePerUnit"],
            joined_df["Qty"],
            joined_df["Date"],
            joined_df["TotalAmount"]
        ).withColumn("IsActive", lit(True)) \
         .withColumn("StartDate", current_timestamp()) \
         .withColumn("EndDate", lit(None).cast(TimestampType())) \
         .withColumn("mergeKey", expr("CONCAT(CustId, '|', OrderId)"))
        
        # Find completely new records (not in existing data)
        new_records = joined_df.join(
            existing_data.select("mergeKey"),
            "mergeKey",
            "left_anti"
        ).select(
            joined_df["CustId"],
            joined_df["Name"],
            joined_df["EmailId"],
            joined_df["Region"],
            joined_df["OrderId"],
            joined_df["ItemName"],
            joined_df["PricePerUnit"],
            joined_df["Qty"],
            joined_df["Date"],
            joined_df["TotalAmount"]
        ).withColumn("IsActive", lit(True)) \
         .withColumn("StartDate", current_timestamp()) \
         .withColumn("EndDate", lit(None).cast(TimestampType())) \
         .withColumn("mergeKey", expr("CONCAT(CustId, '|', OrderId)"))
        
        # Union all records together
        result = updated_existing.union(new_active_records).union(new_records)
        return result.drop("mergeKey")
        
    except:
        # Table doesn't exist yet, create initial data
        return joined_df.withColumn("IsActive", lit(True)) \
                      .withColumn("StartDate", current_timestamp()) \
                      .withColumn("EndDate", lit(None).cast(TimestampType()))

# Step 9-10: Create customer aggregate spend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    return dlt.read("ordersummary") \
        .filter(col("IsActive") == True) \
        .groupBy("Name", "Date") \
        .agg({"TotalAmount": "sum"}) \
        .withColumnRenamed("sum(TotalAmount)", "TotalAmount") \
        .select("Name", "TotalAmount", "Date")