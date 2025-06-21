import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="orders_raw",
    comment="Raw order data with SCD Type 2 and watermarking"
)
def load_orders_raw():
    df = dlt.read("orders_csv")
    df = df.withColumn("CreateDateTime", current_timestamp()) \
           .withColumn("UpdateDateTime", current_timestamp()) \
           .withColumn("IsActive", lit(True))
    df = dlt.watermark("order_date", delay="7 days")  # Example watermarking delay
    return df