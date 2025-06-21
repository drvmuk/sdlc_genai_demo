import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="customer_raw",
    comment="Raw customer data with SCD Type 2"
)
def load_customer_raw():
    df = dlt.read("customer_csv")
    df = df.withColumn("CreateDateTime", current_timestamp()) \
           .netColumn("UpdateDateTime", current_timestamp()) \
           .withColumn("IsActive", lit(True))
    return df