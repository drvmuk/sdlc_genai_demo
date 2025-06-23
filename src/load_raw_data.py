from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Load Raw Data").getOrCreate()

# Load customer raw data into Bronze layer table customer_raw
customer_raw_df = spark.read.csv("/Volumes/catalog_sdlc/rawdata/customer", header=True, inferSchema=True)
customer_raw_df.write.parquet("/bronze/customer_raw", mode="overwrite")

# Load orders raw data into Bronze layer table orders_raw
orders_raw_df = spark.read.csv("/Volumes/catalog_sdlc/rawdata/orders", header=True, inferSchema=True)
orders_raw_df.write.parquet("/bronze/orders_raw", mode="overwrite")

# Create columns for CreateDateTime, UpdateDateTime, and IsActive flag
customer_raw_df = customer_raw_df.withColumn("CreateDateTime", current_timestamp())
customer_raw_df = customer_raw_df.withColumn("UpdateDateTime", current_timestamp())
customer_raw_df = customer_raw_df.withColumn("IsActive", "True")

orders_raw_df = orders_raw_df.withColumn("CreateDateTime", current_timestamp())
orders_raw_df = orders_raw_df.withColumn("UpdateDateTime", current_timestamp())
orders_raw_df = orders_raw_df.withColumn("IsActive", "True")

customer_raw_df.write.parquet("/bronze/customer_raw", mode="overwrite")
orders_raw_df.write.parquet("/bronze/orders_raw", mode="overwrite")

# Stop Spark session
spark.stop()
```

**TR-ETL-002: Ingestion of Data Sources into Silver Layer**