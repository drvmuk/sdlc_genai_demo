"""
Non-DLT version of the pipeline for testing and development purposes.
This file implements the same logic as dlt_pipeline.py but using standard
PySpark DataFrame API instead of Delta Live Tables.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, sum as sum_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Customer Order Pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def process_data(spark):
    # Define schemas for customer and order data
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

    # Define catalog and schema names
    CATALOG = "gen_ai_poc_databrickscoe"
    SCHEMA = "sdlc_wizard"
    
    # Step 1: Read source CSV data from volume
    bronze_customer = (
        spark.read
        .option("header", "true")
        .schema(customer_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    )
    
    bronze_order = (
        spark.read
        .option("header", "true")
        .schema(order_schema)
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )
    
    # Step 2 & 4: Clean customer data - remove nulls and duplicates
    silver_customer = (
        bronze_customer
        .filter(
            col("CustId").isNotNull() &
            col("Name").isNotNull() &
            col("EmailId").isNotNull() &
            col("Region").isNotNull()
        )
        .dropDuplicates(["CustId"])
    )
    
    # Step 3 & 4: Clean order data, add TotalAmount column, remove nulls and duplicates
    silver_order = (
        bronze_order
        .filter(
            col("OrderId").isNotNull() &
            col("ItemName").isNotNull() &
            col("PricePerUnit").isNotNull() &
            col("Qty").isNotNull() &
            col("Date").isNotNull() &
            col("CustId").isNotNull()
        )
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        .dropDuplicates(["OrderId"])
    )
    
    # Step 5: Create ordersummary table if not exists
    spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}
    """)
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ordersummary (
        CustId STRING,
        Name STRING,
        EmailId STRING,
        Region STRING,
        OrderId STRING,
        ItemName STRING,
        PricePerUnit DOUBLE,
        Qty INT,
        Date DATE,
        TotalAmount DOUBLE,
        IsActive BOOLEAN,
        StartDate TIMESTAMP,
        EndDate TIMESTAMP
    )
    USING DELTA
    """)
    
    # Step 6, 7, 8: Process ordersummary table with SCD Type 2
    # Get current data in the table if it exists
    try:
        current_data = spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
        current_data_exists = True
    except:
        current_data_exists = False
    
    # Get new data by joining customer and order
    new_data = (
        silver_customer
        .join(
            silver_order,
            "CustId",
            "inner"
        )
        .select(
            "CustId", 
            "Name", 
            "EmailId", 
            "Region", 
            "OrderId", 
            "ItemName", 
            "PricePerUnit", 
            "Qty", 
            "Date",
            "TotalAmount"
        )
    )
    
    # If table doesn't exist yet, create it with initial data
    if not current_data_exists:
        initial_data = (
            new_data
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )
        initial_data.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ordersummary")
    else:
        # Identify changed records
        customer_changes = (
            silver_customer
            .join(
                current_data.filter(col("IsActive") == True)
                .select("CustId", "Name", "EmailId", "Region"),
                "CustId",
                "inner"
            )
            .filter(
                (col("silver_customer.Name") != col("ordersummary.Name")) |
                (col("silver_customer.EmailId") != col("ordersummary.EmailId")) |
                (col("silver_customer.Region") != col("ordersummary.Region"))
            )
            .select("silver_customer.CustId")
            .distinct()
        )
        
        # Expire old records
        expired_records = (
            current_data
            .join(customer_changes, "CustId", "inner")
            .filter(col("IsActive") == True)
            .withColumn("IsActive", lit(False))
            .withColumn("EndDate", current_timestamp())
        )
        
        # Create new active records
        new_active_records = (
            new_data
            .join(customer_changes, "CustId", "inner")
            .withColumn("IsActive", lit(True))
            .withColumn("StartDate", current_timestamp())
            .withColumn("EndDate", lit(None).cast("timestamp"))
        )
        
        # Combine unchanged records, expired records, and new active records
        unchanged_records = (
            current_data
            .join(customer_changes, "CustId", "left_anti")
        )
        
        # Write updated data back to the table
        updated_data = unchanged_records.union(expired_records).union(new_active_records)
        updated_data.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ordersummary")
    
    # Step 9: Create customeraggregatespend table if not exists
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    )
    USING DELTA
    """)
    
    # Step 10: Aggregate and load data into customeraggregatespend
    current_data = spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
    
    aggregated_data = (
        current_data
        .filter(col("IsActive") == True)  # Only use active records
        .groupBy("Name", "Date")
        .agg(sum_("TotalAmount").alias("TotalAmount"))
    )
    
    aggregated_data.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customeraggregatespend")

if __name__ == "__main__":
    spark = create_spark_session()
    process_data(spark)
    spark.stop()