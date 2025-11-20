import dlt
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import TimestampType

# Define the DLT pipeline for customer and order data processing

@dlt.table(
    name="customer",
    comment="Customer data with data quality rules applied"
)
def customer():
    return (spark.read
            .option("header", "true")
            .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
            .dropDuplicates()
            .filter(col("CustId").isNotNull() & 
                   col("Name").isNotNull() & 
                   col("EmailId").isNotNull() & 
                   col("Region").isNotNull()))

@dlt.table(
    name="order",
    comment="Order data with TotalAmount calculated"
)
def order():
    return (spark.read
            .option("header", "true")
            .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
            .dropDuplicates()
            .filter(col("OrderId").isNotNull() & 
                   col("ItemName").isNotNull() & 
                   col("PricePerUnit").isNotNull() & 
                   col("Qty").isNotNull() & 
                   col("Date").isNotNull() & 
                   col("CustId").isNotNull())
            .withColumn("TotalAmount", col("PricePerUnit") * col("Qty")))

@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table with customer and order data",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_custid", "CustId IS NOT NULL")
def ordersummary():
    # Get the current data
    customer_df = dlt.read("customer")
    order_df = dlt.read("order")
    
    # Join the data
    joined_df = (customer_df.join(order_df, "CustId")
                 .select("CustId", "Name", "EmailId", "Region", 
                         "OrderId", "ItemName", "PricePerUnit", "Qty", "Date"))
    
    # For initial load
    try:
        # Try to read existing data to see if table exists
        existing_df = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
        # If we get here, table exists - implement SCD Type 2 logic
        # First, identify records with changes in customer attributes
        changed_customers = (
            joined_df.alias("source")
            .join(
                existing_df.alias("target").filter(col("IsActive") == True),
                "CustId"
            )
            .where(
                (col("source.Name") != col("target.Name")) |
                (col("source.EmailId") != col("target.EmailId")) |
                (col("source.Region") != col("target.Region"))
            )
            .select("source.CustId")
            .distinct()
        )
        
        # Mark existing records for these customers as inactive
        existing_to_update = (
            existing_df
            .join(changed_customers, "CustId")
            .filter(col("IsActive") == True)
            .withColumn("IsActive", lit(False))
            .withColumn("EndDate", current_timestamp())
        )
        
        # Create new active records for these customers
        new_records = (
            joined_df
            .join(changed_customers, "CustId")
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast(TimestampType()))
        )
        
        # Combine unchanged records, updated records, and new records
        unchanged_records = (
            existing_df
            .join(changed_customers, "CustId", "leftanti")
        )
        
        # Combine all records for the final result
        return unchanged_records.unionAll(existing_to_update).unionAll(new_records)
        
    except:
        # First time load - create with SCD2 fields
        return (joined_df
                .withColumn("IsActive", lit(True))
                .withColumn("StartDate", current_timestamp())
                .withColumn("EndDate", lit(None).cast(TimestampType())))

@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    # Read from the ordersummary table
    ordersummary_df = dlt.read("ordersummary")
    
    # Only use active records for aggregation
    active_records = ordersummary_df.filter(col("IsActive") == True)
    
    # Perform aggregation
    return (active_records
            .groupBy("Name", "Date")
            .agg({"TotalAmount": "sum"})
            .withColumnRenamed("sum(TotalAmount)", "TotalAmount")
            .select("Name", "TotalAmount", "Date"))