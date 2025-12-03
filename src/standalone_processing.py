from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
from delta.tables import DeltaTable

def create_spark_session():
    """Create and return a SparkSession"""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .enableHiveSupport() \
        .getOrCreate()

def process_customer_order_data(spark):
    """
    Process customer and order data according to requirements.
    This is a standalone version that can be run outside of DLT.
    
    Args:
        spark: SparkSession
    """
    # Define schemas
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])

    # Step 1: Read source CSV data
    customer_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(customer_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    )
    
    order_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_schema)
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

    # Step 3 & 4: Clean data and add TotalAmount column
    customer_clean_df = (
        customer_df
        .dropDuplicates()
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
    )
    
    order_clean_df = (
        order_df
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .dropDuplicates()
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
    )

    # Step 5: Create ordersummary table if not exists
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary (
        CustId STRING,
        Name STRING,
        EmailId STRING,
        Region STRING,
        OrderId STRING,
        ItemName STRING,
        PricePerUnit DOUBLE,
        Qty INT,
        Date DATE,
        IsActive BOOLEAN,
        StartDate TIMESTAMP,
        EndDate TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (Date)
    """)

    # Step 6-8: Join customer and order data and load into SCD Type 2 table
    joined_df = (
        order_clean_df
        .join(
            customer_clean_df,
            on="CustId",
            how="inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date"
        )
        .withColumn("IsActive", F.lit(True))
        .withColumn("StartDate", F.current_timestamp())
        .withColumn("EndDate", F.lit(None).cast(TimestampType()))
    )
    
    # Write to ordersummary table (initial load or append new records)
    joined_df.write.format("delta").mode("append").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

    # Step 9: Create customeraggregatespend table if not exists
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    )
    USING DELTA
    """)

    # Step 10: Aggregate and load data
    agg_df = (
        order_clean_df
        .join(
            customer_clean_df,
            on="CustId",
            how="inner"
        )
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )
    
    agg_df.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def main():
    spark = create_spark_session()
    process_customer_order_data(spark)

if __name__ == "__main__":
    main()