"""
Delta Live Tables (DLT) pipeline implementation for the entire ETL process.
Implementation of TR-DLT-007.
"""
import dlt
from pyspark.sql.functions import col, current_timestamp, lit, when, round, date_format, sum as spark_sum
from config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH, 
    SCD_TRACKING_COLUMNS
)

# Step 1: Load customer data from CSV to Delta
@dlt.table(
    name="customer",
    comment="Raw customer data loaded from CSV"
)
def customer():
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(CUSTOMER_DATA_PATH)
    )

# Step 2: Load order data from CSV to Delta
@dlt.table(
    name="order",
    comment="Raw order data loaded from CSV"
)
def order():
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(ORDER_DATA_PATH)
    )

# Step 3: Clean customer data
@dlt.table(
    name="customer_clean",
    comment="Cleaned customer data with nulls and duplicates removed"
)
def customer_clean():
    # Get primary key column (assuming CustId is the primary key)
    primary_key = "CustId"
    
    # Remove rows with null values and duplicates
    return (
        dlt.read("customer")
        .dropna()
        .dropDuplicates([primary_key])
    )

# Step 4: Transform and clean order data
@dlt.table(
    name="order_clean",
    comment="Cleaned and transformed order data with TotalAmount column"
)
def order_clean():
    # Get primary key column (assuming OrderId is the primary key)
    primary_key = "OrderId"
    
    # Add TotalAmount column, remove nulls and duplicates
    return (
        dlt.read("order")
        .withColumn("TotalAmount", round(col("PricePerUnit") * col("Qty"), 2))
        .dropna()
        .dropDuplicates([primary_key])
    )

# Step 5: Create ordersummary table
@dlt.table(
    name="ordersummary",
    comment="Joined customer and order data with SCD Type 2 columns"
)
def ordersummary():
    # Join customer and order data on CustId
    return (
        dlt.read("customer_clean").alias("c")
        .join(
            dlt.read("order_clean").alias("o"),
            col("c.CustId") == col("o.CustId"),
            "inner"
        )
        .select(
            "c.*",
            "o.OrderId",
            "o.OrderDate",
            "o.ProductId",
            "o.ProductName",
            "o.Qty",
            "o.PricePerUnit",
            "o.TotalAmount"
        )
        .withColumn("IsActive", lit(True))
        .withColumn("StartDate", current_timestamp())
        .withColumn("EndDate", lit(None).cast("timestamp"))
    )

# Step 6: Create customeraggregatespend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spend data"
)
def customeraggregatespend():
    # Extract date from OrderDate if it's a timestamp
    order_summary