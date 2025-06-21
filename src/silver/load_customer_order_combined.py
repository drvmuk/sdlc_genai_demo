import dlt
from pyspark.sql.functions import col, sha2, concat_ws
from src.utils.scd2 import apply_scd_type_2

@dlt.table(
    name="customer_order_combined",
    comment="Combined customer and order data with SCD Type 2"
)
def load_customer_order_combined():
    customer_df = dlt.read("bronze.customer_raw")
    orders_df = dlt.read("bronze.orders_raw")
    
    combined_df = customer_df.join(orders_df, on="id", how="inner")
    combined_df = combined_df.dropna()  # Remove nulls
    combined_df = combined_df.withColumn("hash", sha2(concat_ws("|", *combined_df.columns), 256))
    combined_df = combined_df.dropDuplicates(["hash"])  # Deduplicate
    
    combined_df = apply_scd_type_2(combined_df, "id", ["column1", "column2"])  # Example columns for SCD2
    
    return combined_df