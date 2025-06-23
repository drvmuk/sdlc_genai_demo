import dlt
from pyspark.sql.functions import sum, avg

@dlt.table(
    name="customer_order_summary",
    comment="Aggregated customer and order data"
)
def customer_order_summary():
    customer_order_df = dlt.read("customer_order_combined")
    return (
        customer_order_df.groupBy("age")
        .agg(sum("order_amount").alias("total_revenue"), avg("order_amount").alias("average_order_amount"))
    )
