from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def create_bronze_tables(spark: SparkSession):
    # Create bronze tables
    customer_raw = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("path_to_customer_csv")
    orders_raw = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("path_to_orders_csv")

    customer_raw.write.format("delta").mode("overwrite").saveAsTable("bronzezone.data.customer_raw")
    orders_raw.write.format("delta").mode("overwrite").saveAsTable("bronzezone.data.orders_raw")

def create_silver_table(spark: SparkSession):
    # Load bronze tables
    customer_raw = spark.read.table("bronzezone.data.customer_raw")
    orders_raw = spark.read.table("bronzezone.data.orders_raw")

    # Join and apply SCD type 2
    customer_order_combined = customer_raw.join(orders_raw, on="id", how="inner")
    customer_order_combined = customer_order_combined.filter(col("id").isNotNull())
    customer_order_combined = customer_order_combined.dropDuplicates(["id"])

    customer_order_combined.write.format("delta").mode("overwrite").saveAsTable("silverzone.data.customer_order_combined")

def create_gold_table(spark: SparkSession):
    # Load silver table
    customer_order_combined = spark.read.table("silverzone.data.customer_order_combined")

    # Aggregate metrics
    customer_order_summary = customer_order_combined.groupBy("age").agg({"order_amount": "sum", "order_amount": "avg"})

    customer_order_summary.write.format("delta").mode("overwrite").saveAsTable("goldzone.data.customer_order_summary")

def main():
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
    create_bronze_tables(spark)
    create_silver_table(spark)
    create_gold_table(spark)

if __name__ == "__main__":
    main()