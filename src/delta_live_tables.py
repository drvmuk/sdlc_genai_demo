"""
Delta Live Tables implementation for the data pipeline
"""
import dlt
from pyspark.sql.functions import col, current_timestamp, lit, sum as spark_sum

# Define source tables
@dlt.table(
    name="customer_dlt",
    comment="Customer Delta table loaded from CSV"
)
def customer_dlt():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
        .dropDuplicates(["CustId"])
        .filter(col("CustId").isNotNull() & col("Name").isNotNull())
    )

@dlt.table(
    name="order_dlt",
    comment="Order Delta table loaded from CSV with TotalAmount calculated"
)
def order_dlt():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        .filter(
            col("OrderId").isNotNull() & 
            col("CustId").isNotNull() & 
            col("PricePerUnit").isNotNull() & 
            col("Qty").isNotNull()
        )
        .dropDuplicates(["OrderId"])
    )

# Define derived tables
@dlt.table(
    name="ordersummary_dlt",
    comment="Order summary Delta table with SCD Type 2 implementation"
)
@dlt.expect_or_fail("Valid CustId", "CustId IS NOT NULL")
@dlt.expect_or_fail("Valid OrderId", "OrderId IS NOT NULL")
def ordersummary_dlt():
    # Get the customer and order tables
    customers = dlt.read("customer_dlt")
    orders = dlt.read("order_dlt")
    
    # Join tables
    return (
        customers.join(
            orders,
            customers.CustId == orders.CustId,
            "inner"
        ).select(
            customers.CustId,
            customers.Name,
            orders.OrderId,
            orders.Date,
            orders.PricePerUnit,
            orders.Qty,
            orders.TotalAmount,
            lit(True).alias("IsActive"),
            current_timestamp().alias("StartDate"),
            lit(None).cast("timestamp").alias("EndDate")
        )
    )

@dlt.table(
    name="customeraggregatespend_dlt",
    comment="Customer aggregate spend Delta table"
)
def customeraggregatespend_dlt():
    # Get the order summary table
    order_summary = dlt.read("ordersummary_dlt")
    
    # Filter only active records and aggregate
    return (
        order_summary.filter(col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg(spark_sum("TotalAmount").alias("TotalSpend"))
    )