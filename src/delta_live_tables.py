import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when, expr
from pyspark.sql.types import DateType

# Define the source tables as bronze tables
@dlt.table(
    name="customer_bronze",
    comment="Raw customer data from CSV files"
)
def customer_bronze():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")

@dlt.table(
    name="order_bronze",
    comment="Raw order data from CSV files"
)
def order_bronze():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")

# Define the silver tables with cleaned data
@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data with no nulls or duplicates"
)
def customer_silver():
    return dlt.read("customer_bronze") \
        .dropDuplicates(["CustId"]) \
        .filter(col("CustId").isNotNull() & 
                col("Name").isNotNull() & 
                col("EmailId").isNotNull() & 
                col("Region").isNotNull())

@dlt.table(
    name="order_silver",
    comment="Cleaned order data with TotalAmount calculated"
)
def order_silver():
    return dlt.read("order_bronze") \
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty")) \
        .dropDuplicates(["OrderId"]) \
        .filter(col("OrderId").isNotNull() & 
                col("ItemName").isNotNull() & 
                col("PricePerUnit").isNotNull() & 
                col("Qty").isNotNull() & 
                col("Date").isNotNull() & 
                col("CustId").isNotNull())

# Define the order summary table (SCD Type 2)
@dlt.table(
    name="ordersummary",
    comment="Order summary with SCD Type 2 tracking",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_fail("valid_join_key", "CustId IS NOT NULL")
def ordersummary():
    # Get the current data from silver tables
    customer_df = dlt.read("customer_silver")
    order_df = dlt.read("order_silver")
    
    # Join the data
    joined_df = customer_df.join(
        order_df,
        on="CustId",
        how="inner"
    ).select(
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
    
    # Add SCD Type 2 columns
    result_df = joined_df.withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", current_timestamp().cast("date")) \
        .withColumn("EndDate", lit(None).cast(DateType()))
    
    return result_df

# Define the SCD Type 2 update logic using Auto Loader pattern
@dlt.table(
    name="ordersummary_updates",
    comment="Updates to the order summary table",
    temporary=True
)
def ordersummary_updates():
    # Get the current data
    customer_df = dlt.read("customer_silver")
    order_df = dlt.read("order_silver")
    
    # Join the data to create the current state
    current_df = customer_df.join(
        order_df,
        on="CustId",
        how="inner"
    ).select(
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
    
    return current_df

@dlt.table(
    name="ordersummary_scd2",
    comment="SCD Type 2 implementation of order summary",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def ordersummary_scd2():
    from delta.tables import DeltaTable
    import datetime
    
    # Get the current data
    current_data = dlt.read("ordersummary_updates")
    
    # Try to read the existing table
    try:
        existing_table = dlt.read("ordersummary")
        
        # Identify records that need to be updated (have changed)
        changed_records = current_data.join(
            existing_table.filter(col("IsActive") == True),
            on=["CustId", "OrderId"],
            how="inner"
        ).where(
            (current_data["Name"] != existing_table["Name"]) |
            (current_data["EmailId"] != existing_table["EmailId"]) |
            (current_data["Region"] != existing_table["Region"]) |
            (current_data["ItemName"] != existing_table["ItemName"]) |
            (current_data["PricePerUnit"] != existing_table["PricePerUnit"]) |
            (current_data["Qty"] != existing_table["Qty"]) |
            (current_data["Date"] != existing_table["Date"]) |
            (current_data["TotalAmount"] != existing_table["TotalAmount"])
        ).select(
            existing_table["CustId"],
            existing_table["OrderId"]
        )
        
        # Mark existing records as inactive
        updated_existing = existing_table.join(
            changed_records,
            on=["CustId", "OrderId"],
            how="left_outer"
        ).withColumn(
            "IsActive",
            when(changed_records["CustId"].isNotNull(), False).otherwise(existing_table["IsActive"])
        ).withColumn(
            "EndDate",
            when(changed_records["CustId"].isNotNull(), current_timestamp().cast("date")).otherwise(existing_table["EndDate"])
        )
        
        # Create new active records for the changed data
        new_records = current_data.join(
            changed_records,
            on=["CustId", "OrderId"],
            how="inner"
        ).withColumn("IsActive", lit(True)) \
         .withColumn("StartDate", current_timestamp().cast("date")) \
         .withColumn("EndDate", lit(None).cast(DateType()))
        
        # Find completely new records (not in existing table)
        completely_new = current_data.join(
            existing_table.select("CustId", "OrderId"),
            on=["CustId", "OrderId"],
            how="left_anti"
        ).withColumn("IsActive", lit(True)) \
         .withColumn("StartDate", current_timestamp().cast("date")) \
         .withColumn("EndDate", lit(None).cast(DateType()))
        
        # Combine all records
        return updated_existing.union(new_records).union(completely_new)
    except:
        # If the table doesn't exist yet, create it with initial data
        return current_data.withColumn("IsActive", lit(True)) \
            .withColumn("StartDate", current_timestamp().cast("date")) \
            .withColumn("EndDate", lit(None).cast(DateType()))

# Define the customer aggregate spend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    # Read from the order summary table
    order_summary = dlt.read("ordersummary_scd2")
    
    # Aggregate the data
    return order_summary.filter(col("IsActive") == True) \
        .groupBy("Name", "Date") \
        .agg({"TotalAmount": "sum"}) \
        .withColumnRenamed("sum(TotalAmount)", "TotalAmount")