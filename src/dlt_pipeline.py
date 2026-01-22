import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, expr

@dlt.table(
    name="customer",
    comment="Customer data from source files"
)
def customer():
    """Load customer data from source files, remove nulls and duplicates."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
        .na.drop()
        .dropDuplicates(["CustId"])
    )

@dlt.table(
    name="order",
    comment="Order data from source files with TotalAmount calculated"
)
def order():
    """Load order data from source files, add TotalAmount, remove nulls and duplicates."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
        .na.drop()
        .dropDuplicates(["OrderId"])
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    )

@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table joining customer and order data",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_fail("valid_custid", "CustId IS NOT NULL")
def ordersummary():
    """
    Create SCD Type 2 table joining customer and order data.
    This function handles both initial load and updates.
    """
    # Get the latest customer and order data
    customer_df = dlt.read("customer")
    order_df = dlt.read("order")
    
    # Join the data
    joined_df = customer_df.join(order_df, "CustId", "inner").select(
        "CustId", 
        "Name", 
        "EmailId", 
        "Region", 
        "OrderId", 
        "ItemName", 
        "PricePerUnit", 
        "Qty", 
        "Date"
    )
    
    # Add SCD Type 2 fields
    result_df = joined_df \
        .withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", current_timestamp()) \
        .withColumn("EndDate", lit(None).cast("timestamp"))
    
    # Check if the table already exists
    try:
        # If the table exists, we need to perform SCD Type 2 operations
        existing_df = dlt.read("ordersummary")
        
        # Identify changed records
        changed_records = existing_df.alias("existing").join(
            joined_df.alias("new"),
            (col("existing.CustId") == col("new.CustId")) &
            (col("existing.OrderId") == col("new.OrderId")) &
            col("existing.IsActive"),
            "inner"
        ).where(
            (col("existing.Name") != col("new.Name")) |
            (col("existing.EmailId") != col("new.EmailId")) |
            (col("existing.Region") != col("new.Region")) |
            (col("existing.ItemName") != col("new.ItemName")) |
            (col("existing.PricePerUnit") != col("new.PricePerUnit")) |
            (col("existing.Qty") != col("new.Qty")) |
            (col("existing.Date") != col("new.Date"))
        ).select("existing.CustId", "existing.OrderId")
        
        # Expire old records
        expired_records = existing_df.join(
            changed_records,
            ["CustId", "OrderId"],
            "inner"
        ).withColumn("IsActive", lit(False)) \
         .withColumn("EndDate", current_timestamp())
        
        # Create new active records
        new_active_records = joined_df.join(
            changed_records,
            ["CustId", "OrderId"],
            "inner"
        ).withColumn("IsActive", lit(True)) \
         .withColumn("StartDate", current_timestamp()) \
         .withColumn("EndDate", lit(None).cast("timestamp"))
        
        # Find completely new records
        new_records = joined_df.join(
            existing_df.select("CustId", "OrderId").dropDuplicates(),
            ["CustId", "OrderId"],
            "left_anti"
        ).withColumn("IsActive", lit(True)) \
         .withColumn("StartDate", current_timestamp()) \
         .withColumn("EndDate", lit(None).cast("timestamp"))
        
        # Unchanged records
        unchanged_records = existing_df.join(
            changed_records,
            ["CustId", "OrderId"],
            "left_anti"
        )
        
        # Combine all records
        return unchanged_records.union(expired_records).union(new_active_records).union(new_records)
        
    except Exception as e:
        # If table doesn't exist yet, return the initial data
        return result_df

@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    """
    Aggregate the TotalAmount by customer name and date.
    """
    # Get the order summary data
    order_summary_df = dlt.read("ordersummary")
    
    # Calculate TotalAmount if it doesn't exist
    if "TotalAmount" not in order_summary_df.columns:
        order_summary_df = order_summary_df.withColumn(
            "TotalAmount", 
            col("PricePerUnit") * col("Qty")
        )
    
    # Aggregate the data
    return order_summary_df \
        .filter(col("IsActive") == True) \
        .groupBy("Name", "Date") \
        .agg({"TotalAmount": "sum"}) \
        .withColumnRenamed("sum(TotalAmount)", "TotalAmount")