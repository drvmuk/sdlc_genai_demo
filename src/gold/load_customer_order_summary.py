import dlt
from pyspark.sql.functions import sum, avg, col
from src.utils.scd2 import apply_scd_type_2

@dlt.table(
    name="customer_order_summary",
    comment="Aggregated customer order summary with SCD Type 2"
)
def load_customer_order_summary():
    combined_df = dlt.read("silver.customer_order_combined")
    
    summary_df = combined_df.groupBy("age").agg(sum("order_amount").alias("TotalRevenue"), avg("order_amount").alias("AverageOrderAmount"))
    summary_df = apply_scd_type_2(summary_df, "age", ["TotalRevenue", "AverageOrderAmount"])
    
    return summary_df