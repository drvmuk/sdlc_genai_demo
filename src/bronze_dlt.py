# src/bronze_dlt.py
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
    name="customer_raw",
    comment="Delta table containing customer data from CSV, with SCD type-2 history"
)
def load_customer_data():
    """
    Ingests customer data from a CSV file in a Unity Catalog Volume into a Delta table,
    applying SCD type-2 logic, history tracking, watermark columns, and activity flags.
    """
    spark = dlt.spark
    catalog = "bronzezone"
    schema = "data"
    table = f"{catalog}.{schema}.customer_raw"
    csv_path = "/Volumes/catalog_sdlc/rawdata/customer"

    check_catalog_schema_table(spark, catalog, schema, table)

    raw_df = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("header", "true")
              .option("cloudFiles.inferColumnTypes", "true") # Enable schema inference
              .load(csv_path))

    df = raw_df.withColumn("CreateDateTime", F.current_timestamp()) \
               .withColumn("UpdateDateTime", F.current_timestamp()) \
               .withColumn("IsActive", F.lit(True))

    # SCD Type 2 logic using MERGE
    customer_raw_temp_view = "customer_raw_temp_view"
    df.createOrReplaceTempView(customer_raw_temp_view)

    if check_catalog_schema_table(spark, catalog, schema, table):
      merge_sql = f"""
        MERGE INTO {table} AS target
        USING {customer_raw_temp_view} AS source
        ON target.id = source.id
        WHEN MATCHED AND (target.name <> source.name OR target.email <> source.email OR target.age <> source.age) AND target.IsActive = true THEN
          UPDATE SET target.IsActive = false, target.UpdateDateTime = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT *
        """
      spark.sql(merge_sql)
    else:
        df.write.format("delta").option("mergeSchema", "true").saveAsTable(table)

    # Return the current active records
    return spark.table(table).filter(F.col("IsActive") == True)


@dlt.table(
    name="orders_raw",
    comment="Delta table containing order data from CSV, with SCD type-2 history"
)
def load_order_data():
    """
    Ingests order data from a CSV file in a Unity Catalog Volume into a Delta table,
    applying SCD type-2 logic, history tracking, watermark columns, and activity flags.
    """
    spark = dlt.spark
    catalog = "bronzezone"
    schema = "data"
    table = f"{catalog}.{schema}.orders_raw"
    csv_path = "/Volumes/catalog_sdlc/rawdata/order"

    check_catalog_schema_table(spark, catalog, schema, table)

    raw_df = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("header", "true")
              .option("cloudFiles.inferColumnTypes", "true") # Enable schema inference
              .load(csv_path))


    df = raw_df.withColumn("CreateDateTime", F.current_timestamp()) \
               .withColumn("UpdateDateTime", F.current_timestamp()) \
               .withColumn("IsActive", F.lit(True))

    # SCD Type 2 logic using MERGE
    orders_raw_temp_view = "orders_raw_temp_view"
    df.createOrReplaceTempView(orders_raw_temp_view)

    if check_catalog_schema_table(spark, catalog, schema, table):
      merge_sql = f"""
        MERGE INTO {table} AS target
        USING {orders_raw_temp_view} AS source
        ON target.id = source.id
        WHEN MATCHED AND (target.order_amount <> source.order_amount OR target.order_date <> source.order_date) AND target.IsActive = true THEN
          UPDATE SET target.IsActive = false, target.UpdateDateTime = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT *
        """
      spark.sql(merge_sql)
    else:
        df.write.format("delta").option("mergeSchema", "true").saveAsTable(table)

    # Return the current active records
    return spark.table(table).filter(F.col("IsActive") == True)
