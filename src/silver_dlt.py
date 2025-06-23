# src/silver_dlt.py
import dlt
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

def check_catalog_schema_table(spark, catalog, schema, table):
    """Checks if catalog, schema, and table exist; creates them if they don't."""
    try:
        spark.sql(f"USE CATALOG {catalog}")
    except AnalysisException:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"USE CATALOG {catalog}")

    try:
        spark.sql(f"USE SCHEMA {schema}")
    except AnalysisException:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        spark.sql(f"USE SCHEMA {schema}")

    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 1")
    except AnalysisException:
        return False  # Table doesn't exist
    return True


@dlt.table(
    name="customer_order_combined",
    comment="Delta table containing joined and cleansed customer and order data, with SCD type-2 history"
)
def create_customer_order_combined():
    """
    Joins and cleanses data from customer_raw and orders_raw, applying SCD type-2 logic.
    """
    spark = dlt.spark
    catalog = "silverzone"
    schema = "data"
    table = f"{catalog}.{schema}.customer_order_combined"

    check_catalog_schema_table(spark, catalog, schema, table)

    customer_df = dlt.read("customer_raw")
    order_df = dlt.read("orders_raw")

    joined_df = customer_df.join(order_df, "id", "inner") \
                           .dropna() \
                           .dropDuplicates()

    df = joined_df.withColumn("CreateDateTime", F.current_timestamp()) \
                   .withColumn("UpdateDateTime", F.current_timestamp()) \
                   .withColumn("IsActive", F.lit(True))

    # SCD Type 2 logic using MERGE
    customer_order_combined_temp_view = "customer_order_combined_temp_view"
    df.createOrReplaceTempView(customer_order_combined_temp_view)

    if check_catalog_schema_table(spark, catalog, schema, table):
      merge_sql = f"""
        MERGE INTO {table} AS target
        USING {customer_order_combined_temp_view} AS source
        ON target.id = source.id
        WHEN MATCHED AND (target.name <> source.name OR target.email <> source.email OR target.age <> source.age OR target.order_amount <> source.order_amount OR target.order_date <> source.order_date) AND target.IsActive = true THEN
          UPDATE SET target.IsActive = false, target.UpdateDateTime = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT *
        """
      spark.sql(merge_sql)
    else:
        df.write.format("delta").option("mergeSchema", "true").saveAsTable(table)

    # Return the current active records
    return spark.table(table).filter(F.col("IsActive") == True)
