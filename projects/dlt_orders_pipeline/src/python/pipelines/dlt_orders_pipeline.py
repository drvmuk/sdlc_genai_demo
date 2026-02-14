import dlt
from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame

CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

CUSTOMER_SOURCE = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_SOURCE = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

customer_schema = T.StructType([
    T.StructField("CustId", T.StringType(), True),
    T.StructField("Name", T.StringType(), True),
    T.StructField("EmailId", T.StringType(), True),
    T.StructField("Region", T.StringType(), True),
])

order_schema = T.StructType([
    T.StructField("OrderId", T.StringType(), True),
    T.StructField("ItemName", T.StringType(), True),
    T.StructField("PricePerUnit", T.DecimalType(18, 2), True),
    T.StructField("Qty", T.LongType(), True),
    T.StructField("Date", T.DateType(), True),
    T.StructField("CustId", T.StringType(), True),
])

NORMALIZE_NULLS = ["null", "NULL", "Null", "none", "", "N/A", "na", "Na", "NA"]

@dlt.view(name=f"{SCHEMA}_customer_src")
def customer_src():
    df = spark.read.format("csv").schema(customer_schema).option("header", True).load(CUSTOMER_SOURCE)
    df = df.withColumn("_ingest_ts", F.current_timestamp())
    return df

@dlt.view(name=f"{SCHEMA}_order_src")
def order_src():
    df = spark.read.format("csv").schema(order_schema).option("header", True).load(ORDER_SOURCE)
    df = df.withColumn("_ingest_ts", F.current_timestamp())
    return df


def normalize_nulls(df: DataFrame) -> DataFrame:
    for c in df.columns:
        if c.startswith("_"):
            continue
        df = df.withColumn(c, F.when(F.trim(F.col(c)).isin(NORMALIZE_NULLS), F.lit(None)).otherwise(F.col(c)))
    return df

@dlt.table(name=f"{SCHEMA}_customer", comment="Raw customer enforced schema", path=None)
@dlt.expect_or_drop("custid_not_null", "CustId IS NOT NULL")
def customer():
    df = dlt.read(f"{SCHEMA}_customer_src")
    return df.select("CustId", "Name", "EmailId", "Region", "_ingest_ts")

@dlt.table(name=f"{SCHEMA}_order", comment="Raw order enforced schema", path=None)
@dlt.expect_or_drop("orderid_not_null", "OrderId IS NOT NULL")
@dlt.expect_or_drop("custid_not_null", "CustId IS NOT NULL")
@dlt.expect_or_drop("date_not_null", "Date IS NOT NULL")
def order():
    df = dlt.read(f"{SCHEMA}_order_src")
    return df.select("OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId", "_ingest_ts")

@dlt.table(name=f"{SCHEMA}_customer_clean", comment="Cleansed and deduplicated customers")
def customer_clean():
    df = dlt.read(f"{SCHEMA}_customer")
    df = normalize_nulls(df)
    df = df.dropna(subset=["CustId"])  # required
    w = F.window("_ingest_ts", "36500 days")  # placeholder, not used
    win = F.window("_ingest_ts", "1 seconds")
    # dedupe by CustId keeping latest by _ingest_ts
    from pyspark.sql.window import Window
    w = Window.partitionBy("CustId").orderBy(F.col("_ingest_ts").desc())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    return df.select("CustId", "Name", "EmailId", "Region", "_ingest_ts")

@dlt.table(name=f"{SCHEMA}_order_refined", comment="Cleansed, deduped orders with TotalAmount")
@dlt.expect_or_drop("orderid_not_null", "OrderId IS NOT NULL")
@dlt.expect_or_drop("custid_not_null", "CustId IS NOT NULL")
@dlt.expect_or_drop("date_not_null", "Date IS NOT NULL")
@dlt.expect_or_drop("totalamount_non_negative", "(PricePerUnit * Qty) >= 0")
def order_refined():
    df = dlt.read(f"{SCHEMA}_order")
    df = normalize_nulls(df)
    df = df.dropna(subset=["OrderId", "CustId", "Date"])  # required
    from pyspark.sql.window import Window
    w = Window.partitionBy("OrderId").orderBy(F.col("_ingest_ts").desc())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    df = df.withColumn("TotalAmount", (F.col("PricePerUnit") * F.col("Qty")).cast(T.DecimalType(18, 2)))
    return df.select("OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId", "TotalAmount", "_ingest_ts")

@dlt.table(name=f"{SCHEMA}_order_clean", comment="Alias of order_refined for clarity")
@dlt.expect_or_drop("totalamount_non_negative", "TotalAmount >= 0")
def order_clean():
    return dlt.read(f"{SCHEMA}_order_refined")

@dlt.table(name=f"{SCHEMA}_order_enriched", comment="Join customer and order")
def order_enriched():
    cust = dlt.read(f"{SCHEMA}_customer_clean").alias("c")
    ords = dlt.read(f"{SCHEMA}_order_refined").alias("o")
    j = ords.join(cust, F.col("o.CustId") == F.col("c.CustId"), "inner")
    return j.select(
        F.col("o.CustId").alias("CustId"),
        F.col("c.Name").alias("Name"),
        F.col("c.EmailId").alias("EmailId"),
        F.col("c.Region").alias("Region"),
        F.col("o.OrderId").alias("OrderId"),
        F.col("o.ItemName").alias("ItemName"),
        F.col("o.PricePerUnit").alias("PricePerUnit"),
        F.col("o.Qty").alias("Qty"),
        F.col("o.Date").alias("Date"),
        F.col("o.TotalAmount").alias("TotalAmount"),
        F.col("o._ingest_ts").alias("_ingest_ts")
    )

# SCD2 Ordersummary
@dlt.table(name=f"{SCHEMA}_ordersummary", comment="SCD2 order summary table")
@dlt.expect_or_drop("totalamount_non_negative", "TotalAmount >= 0")
def ordersummary():
    src = dlt.read(f"{SCHEMA}_order_enriched").withColumn(
        "RecordHash",
        F.sha2(F.concat_ws(
            "||",
            F.coalesce(F.col("Name"), F.lit("")),
            F.coalesce(F.col("EmailId"), F.lit("")),
            F.coalesce(F.col("Region"), F.lit("")),
            F.coalesce(F.col("ItemName"), F.lit("")),
            F.coalesce(F.col("PricePerUnit").cast("string"), F.lit("")),
            F.coalesce(F.col("Qty").cast("string"), F.lit("")),
            F.coalesce(F.date_format(F.col("Date"), "yyyy-MM-dd"), F.lit("")),
            F.coalesce(F.col("TotalAmount").cast("string"), F.lit(""))
        ), 256)
    )

    existing = dlt.read_stream(f"{SCHEMA}_ordersummary_current").selectExpr("*")

    # Prepare candidate rows with SCD2 columns
    now_ts = F.current_timestamp()
    staged = src.select(
        "CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount", "RecordHash"
    ).withColumn("IsActive", F.lit(True)) \
     .withColumn("StartDate", now_ts) \
     .withColumn("EndDate", F.lit(None).cast(T.TimestampType())) \
     .withColumn("SurrogateKey", F.sha2(F.concat_ws("-", F.col("CustId"), F.col("OrderId"), F.col("StartDate").cast("string")), 256))

    # Merge for SCD2
    # Business key: CustId, OrderId
    # Change when RecordHash differs vs active record
    merge_source = staged.alias("s")

    return dlt.apply_changes(
        target = f"{SCHEMA}_ordersummary_current",
        source = merge_source,
        keys = ["CustId", "OrderId"],
        sequence_by = "StartDate",
        stored_as_scd_type = 2,
        track_history_column_list=["Name", "EmailId", "Region", "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount", "RecordHash", "SurrogateKey"],
        except_column_list=["IsActive", "StartDate", "EndDate"],
    )

@dlt.table(name=f"{SCHEMA}_ordersummary_current", comment="Backing table for SCD2 ordersummary")
def ordersummary_current():
    # seed empty table for apply_changes target
    empty_schema = T.StructType([
        T.StructField("CustId", T.StringType(), True),
        T.StructField("Name", T.StringType(), True),
        T.StructField("EmailId", T.StringType(), True),
        T.StructField("Region", T.StringType(), True),
        T.StructField("OrderId", T.StringType(), True),
        T.StructField("ItemName", T.StringType(), True),
        T.StructField("PricePerUnit", T.DecimalType(18, 2), True),
        T.StructField("Qty", T.LongType(), True),
        T.StructField("Date", T.DateType(), True),
        T.StructField("TotalAmount", T.DecimalType(18, 2), True),
        T.StructField("IsActive", T.BooleanType(), True),
        T.StructField("StartDate", T.TimestampType(), True),
        T.StructField("EndDate", T.TimestampType(), True),
        T.StructField("RecordHash", T.StringType(), True),
        T.StructField("SurrogateKey", T.StringType(), True),
    ])
    return spark.createDataFrame([], empty_schema)

@dlt.table(name=f"{SCHEMA}_customeraggregatespend", comment="Aggregate spend by customer name and date from active SCD2")
def customeraggregatespend():
    cur = dlt.read(f"{SCHEMA}_ordersummary_current").filter(F.col("IsActive") == True)
    agg = cur.groupBy("Name", "Date").agg(F.sum("TotalAmount").cast(T.DecimalType(18,2)).alias("TotalSpend"))
    # Upsert semantics by Name+Date handled implicitly by DLT table materialization; in production use apply_changes for idempotency if needed.
    return agg
