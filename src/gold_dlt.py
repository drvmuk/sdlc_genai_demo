# src/gold_dlt.py
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

grouping_column = "age"  # configurable, can be passed as parameter to the DLT pipeline

@dlt.table(
    name="customer_order_summary",
    comment="Delta table containing aggregated customer and order data, with SCD type-2 history"
)
def create_customer_order_summary():
    """
    Aggregates data from customer_order_combined by age or email domain, applying SCD type-2 logic.
    """
    spark = dlt.spark
    catalog = "goldzone"
    schema = "data"
    table = f"{catalog}.{schema}.customer_order_summary"

    check_catalog_schema_table(spark, catalog, schema, table)

    df = dlt.read("customer_order_combined")

    # Group by either age or email domain (configurable)
    if grouping_column == "age":
        grouped_df = df.groupBy("age").agg(
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("average_order_amount")
        )
    elif grouping_column == "email_domain":
        grouped_df = df.withColumn("email_domain", F.split(F.col("email"), "@").getItem(1)) \
                       .groupBy("email_domain").agg(
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("average_order_amount")
        )
    else:
        raise ValueError("Invalid grouping column. Must be 'age' or 'email_domain'.")

    df_with_metadata = grouped_df.withColumn("CreateDateTime", F.current_timestamp()) \
                                 .withColumn("UpdateDateTime", F.current_timestamp()) \
                                 .withColumn("IsActive", F.lit(True))

    # SCD Type 2 logic using MERGE
    customer_order_summary_temp_view = "customer_order_summary_temp_view"
    df_with_metadata.createOrReplaceTempView(customer_order_summary_temp_view)

    if check_catalog_schema_table(spark, catalog, schema, table):
      merge_sql = f"""
        MERGE INTO {table} AS target
        USING {customer_order_summary_temp_view} AS source
        ON target.{grouping_column} = source.{grouping_column}
        WHEN MATCHED AND (target.total_revenue <> source.total_revenue OR target.average_order_amount <> source.average_order_amount) AND target.IsActive = true THEN
          UPDATE SET target.IsActive = false, target.UpdateDateTime = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT *
        """
      spark.sql(merge_sql)
    else:
        df_with_metadata.write.format("delta").option("mergeSchema", "true").saveAsTable(table)


    # Return the current active records
    return spark.table(table).filter(F.col("IsActive") == True)
