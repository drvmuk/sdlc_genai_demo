import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="customer_order_combined",
    comment="Transformed customer and order data"
)
def customer_order_combined():
    customer_df = dlt.read("customer_raw")
    orders_df = dlt.read("orders_raw")
    return (
        customer_df.join(orders_df, on="id", how="inner")
        .filter(col("id").isNotNull())
        .dropDuplicates(["id"])
    )
