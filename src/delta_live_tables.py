"""
Delta Live Tables implementation for customer and order data processing.
"""
import dlt
from pyspark.sql.functions import col, lit, current_timestamp, when
from pyspark.sql.types import TimestampType


@dlt.table(
    name="customer_bronze",
    comment="Raw customer data from source"
)
def customer_bronze():
    return spark.read.format("csv") \
        .option("header", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")


@dlt.table(
    name="order_bronze",
    comment="Raw order data from source"
)
def order_bronze():
    return spark.read.format("csv") \
        .option("header", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")


@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data"
)
def customer_silver():
    return dlt.read("customer_bronze") \
        .dropDuplicates() \
        .na.drop()


@dlt.table(
    name="order_silver",
    comment="Cleaned order data with TotalAmount"
)
def order_silver():
    return dlt.read("order_bronze") \
        .dropDuplicates() \
        .na.drop() \
        .withColumn("PricePerUnit", col("PricePerUnit").cast("double")) \
        .withColumn("Qty", col("Qty").cast("int")) \
        .withColumn("Date", col("Date").cast("date")) \
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))


@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table with customer and order data",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
def ordersummary():
    # Get current data
    customer_df = dlt.read("customer_silver")
    order_df = dlt.read("order_silver")
    
    # Join customer and order data
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
    current_time = current_timestamp()
    new_data = joined_df.withColumn("IsActive", lit(True)) \
                        .withColumn("StartDate", current_time) \
                        .withColumn("EndDate", lit(None).cast(TimestampType()))
    
    # Check if we have existing data
    try:
        # Get existing data
        existing_data = dlt.read("ordersummary")
        
        # Identify records that need to be updated (where customer details have changed)
        records_to_update = existing_data.alias("existing").join(
            new_data.alias("new"),
            (existing_data["CustId"] == new_data["CustId"]) &
            (existing_data["OrderId"] == new_data["OrderId"]) &
            (existing_data["IsActive"] == True) &
            (
                (existing_data["Name"] != new_data["Name"]) |
                (existing_data["EmailId"] != new_data["EmailId"]) |
                (existing_data["Region"] != new_data["Region"])
            ),
            "inner"
        ).select("existing.*")
        
        # Mark old records as inactive
        updated_existing = existing_data.join(
            records_to_update,
            on=["CustId", "OrderId", "IsActive"],
            how="left_anti"
        ).union(
            records_to_update.withColumn("IsActive", lit(False))
                            .withColumn("EndDate", current_time)
        )
        
        # Combine with new data
        return updated_existing.union(new_data)
    except:
        # If the table doesn't exist yet, just return the new data
        return new_data


@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spend by name and date"
)
def customeraggregatespend():
    # Read from the ordersummary table
    ordersummary_df = dlt.read("ordersummary")
    
    # Aggregate the data
    return ordersummary_df.filter(col("IsActive") == True) \
                         .groupBy("Name", "Date") \
                         .sum("TotalAmount") \
                         .withColumnRenamed("sum(TotalAmount)", "TotalAmount")