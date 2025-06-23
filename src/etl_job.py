from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp
from delta.tables import *

# Create a SparkSession
spark = SparkSession.builder.appName("ETL Job").getOrCreate()

# Load raw Customer data from Unity Catalog volume
customer_raw_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("unity-catalog-volume/customer")

# Apply SCD type-2 logic to the customer data
customer_raw_data = customer_raw_data.withColumn("effective_date", current_timestamp()).withColumn("end_date", when(col("id").isNull(), current_timestamp()).otherwise(col("end_date")))

# Create the "customer_raw" table with SCD type-2, keeping history, and using watermark columns
customer_raw_table = DeltaTable.createIfNotExists(spark, "bronzezone.data.customer_raw") \
    .addColumn("id", "string") \
    .addColumn("effective_date", "timestamp") \
    .addColumn("end_date", "timestamp") \
    .withColumn("watermark", col("effective_date"))

# Load raw Order data from Unity Catalog volume
order_raw_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("unity-catalog-volume/order")

# Apply SCD type-2 logic to the order data
order_raw_data = order_raw_data.withColumn("effective_date", current_timestamp()).withColumn("end_date", when(col("id").isNull(), current_timestamp()).otherwise(col("end_date")))

# Create the "orders_raw" table with SCD type-2, keeping history, and using watermark columns
orders_raw_table = DeltaTable.createIfNotExists(spark, "bronzezone.data.orders_raw") \
    .addColumn("id", "string") \
    .addColumn("effective_date", "timestamp") \
    .addColumn("end_date", "timestamp") \
    .withColumn("watermark", col("effective_date"))

# Insert data into the "customer_raw" table
customer_raw_data.write.format("delta").mode("append").save("bronzezone.data.customer_raw")

# Insert data into the "orders_raw" table
order_raw_data.write.format("delta").mode("append").save("bronzezone.data.orders_raw")