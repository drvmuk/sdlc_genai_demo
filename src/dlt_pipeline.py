import dlt
from pyspark.sql.functions import col, current_timestamp, lit, when, expr, sum as spark_sum

# Define the DLT pipeline for customer and order data processing

# Source tables
@dlt.table(
    name="customer_bronze",
    comment="Raw customer data from CSV"
)
def customer_bronze():
    return spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")

@dlt.table(
    name="order_bronze",
    comment="Raw order data from CSV"
)
def order_bronze():
    return spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")

# Silver tables - cleaned data
@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data with no nulls or duplicates"
)
def customer_silver():
    return dlt.read("customer_bronze") \
        .na.drop() \
        .dropDuplicates()

@dlt.table(
    name="order_silver",
    comment="Cleaned order data with TotalAmount calculated"
)
def order_silver():
    return dlt.read("order_bronze") \
        .na.drop() \
        .dropDuplicates() \
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))

# Gold tables - business value
@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table with customer and order data",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_custid": "CustId IS NOT NULL",
    "valid_orderid": "OrderId IS NOT NULL"
})
def ordersummary():
    # Read the latest silver data
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
    
    # Add SCD Type 2 columns for new data
    new_data = joined_df.withColumn("IsActive", lit(True)) \
                      .withColumn("StartDate", current_timestamp()) \
                      .withColumn("EndDate", lit(None))
    
    # Check if the target table exists
    try:
        # Try to read from the target table
        existing_data = dlt.read("ordersummary")
        
        # Identify changed records (where customer data changed)
        changed_records = new_data.join(
            existing_data.filter(col("IsActive") == True),
            "CustId",
            "inner"
        ).where(
            (new_data["Name"] != existing_data["Name"]) | 
            (new_data["EmailId"] != existing_data["EmailId"]) | 
            (new_data["Region"] != existing_data["Region"])
        ).select(new_data["CustId"]).distinct()
        
        # Mark existing active records as inactive if they've changed
        updated_existing = existing_data.join(
            changed_records, 
            "CustId", 
            "left_outer"
        ).withColumn(
            "IsActive", 
            when(changed_records["CustId"].isNotNull() & (existing_data["IsActive"] == True), 
                 lit(False)
            ).otherwise(existing_data["IsActive"])
        ).withColumn(
            "EndDate",
            when(changed_records["CustId"].isNotNull() & (existing_data["IsActive"] == True),
                 current_timestamp()
            ).otherwise(existing_data["EndDate"])
        )
        
        # Union the updated existing data with new data
        return updated_existing.union(new_data)
    
    except Exception as e:
        # If table doesn't exist yet, return just the new data
        return new_data

@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    # Read from ordersummary table
    ordersummary_df = dlt.read("ordersummary")
    
    # Aggregate data - only use active records
    return ordersummary_df.filter(col("IsActive") == True) \
        .groupBy("Name", "Date") \
        .agg(spark_sum("TotalAmount").alias("TotalAmount"))