from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Ingest Data").getOrCreate()

# Load customer raw data from Bronze layer table customer_raw into Silver layer table customer_order_combined
customer_raw_df = spark.read.parquet("/bronze/customer_raw")
orders_raw_df = spark.read.parquet("/bronze/orders_raw")

# Join customer and orders data on the id column
customer_order_combined_df = customer_raw_df.join(orders_raw_df, "id", "inner")

# Remove records with Null values
customer_order_combined_df = customer_order_combined_df.na.drop(subset=None)

# Remove duplicate records
customer_order_combined_df = customer_order_combined_df.dropDuplicates(subset=None)

# Apply SCD type 2 using id as primary key, keeping history, using watermark columns
customer_order_combined_df = customer_order_combined_df.withColumn("SCD_Start", current_timestamp())

customer_order_combined_df.write.parquet("/silver/customer_order_combined", mode="overwrite")

# Stop Spark session
spark.stop()
```

**TR-ETL-003: Load Data to Gold Layer**