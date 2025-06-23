import dlt
from pyspark.sql.functions import col, current_timestamp, lit

@dlt.table(
    name="customer_raw",
    comment="Raw customer data with SCD type-2"
)
def customer_raw():
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load("path_to_customer_data_csv")
        .withColumn("watermark", current_timestamp())
        .withColumn("is_active", lit(True))
    )

@dlt.table(
    name="orders_raw",
    comment="Raw order data with SCD type-2"
)
def orders_raw():
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load("path_to_order_data_csv")
        .withColumn("watermark", current_timestamp())
        .withColumn("is_active", lit(True))
    )
