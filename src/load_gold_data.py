from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Load Gold Data").getOrCreate()

# Load combined customer and orders data from Silver layer table customer_order_combined into Gold layer table customer_order_summary
customer_order_combined_df = spark.read.parquet("/silver/customer_order_combined")

# Group by age or email domain
customer_order_summarized_df = customer_order_combined_df.groupBy("age" | "email_domain")

# Aggregate metrics like total revenue and average order amount
customer_order_summarized_df = customer_order_summarized_df.agg({"total_revenue": "sum", "avg_order_amount": "avg"})

customer_order_summarized_df.write.parquet("/gold/customer_order_summary", mode="overwrite")

# Stop Spark session
spark.stop()
```

**TR-ETL-004: Generation of Metadata and Tests**