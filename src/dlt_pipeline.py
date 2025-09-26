import dlt
from pyspark.sql.functions import col, current_timestamp, lit, expr

@dlt.table(
    name="customer_bronze",
    comment="Raw customer data from source"
)
def customer_bronze():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    )

@dlt.table(
    name="order_bronze",
    comment="Raw order data from source"
)
def order_bronze():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data with nulls and duplicates removed"
)
def customer_silver():
    return (
        dlt.read("customer_bronze")
        .dropna()
        .dropDuplicates()
    )

@dlt.table(
    name="order_silver",
    comment="Cleaned order data with TotalAmount calculated"
)
def order_silver():
    return (
        dlt.read("order_bronze")
        .dropna()
        .dropDuplicates()
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    )

@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table combining customer and order data",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
def ordersummary():
    # Read current data if it exists
    try:
        current_data = dlt.read("ordersummary")
        has_current_data = True
    except:
        has_current_data = False
    
    # Join customer and order data
    joined_data = (
        dlt.read("customer_silver")
        .join(dlt.read("order_silver"), "CustId")
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
        )
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None))
    )
    
    if has_current_data:
        # Identify records that have changed
        changed_records = (
            joined_data.alias("new")
            .join(
                current_data.filter(col("IsActive") == True).alias("current"),
                (col("new.CustId") == col("current.CustId")) &
                (col("new.OrderId") == col("current.OrderId")),
                "left_anti"
            )
        )
        
        # Mark existing records as inactive
        updated_current = (
            current_data.alias("current")
            .join(
                joined_data.alias("new"),
                (col("current.CustId") == col("new.CustId")) &
                (col("current.OrderId") == col("new.OrderId")) &
                (
                    (col("current.Name") != col("new.Name")) |
                    (col("current.EmailId") != col("new.EmailId")) |
                    (col("current.Region") != col("new.Region"))
                ),
                "left"
            )
            .withColumn(
                "IsActive", 
                expr("CASE WHEN new.CustId IS NOT NULL THEN false ELSE current.IsActive END")
            )
            .withColumn(
                "EndDate",
                expr("CASE WHEN new.CustId IS NOT NULL THEN current_timestamp() ELSE current.EndDate END")
            )
            .select("current.*")
        )
        
        # Union the updated current records with the new records
        return updated_current.union(changed_records)
    else:
        # If no current data, return all as new
        return joined_data

@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spend by name and date"
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .filter(col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg({"TotalAmount": "sum"})
        .withColumnRenamed("sum(TotalAmount)", "TotalAmount")
        .select("Name", "TotalAmount", "Date")
    )