"""
Batch processing version of the pipeline for testing purposes.
This is not used in the DLT implementation but provides a way to test the logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, BooleanType, TimestampType

from config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH, 
    TARGET_CATALOG, TARGET_SCHEMA,
    CUSTOMER_TABLE, ORDER_TABLE, ORDER_SUMMARY_TABLE, CUSTOMER_AGGREGATE_SPEND_TABLE
)

def create_spark_session():
    """Create a Spark session for batch processing."""
    return SparkSession.builder \
        .appName("Customer Order Batch Processing") \
        .enableHiveSupport() \
        .getOrCreate()

def process_customer_data(spark):
    """Process customer data from source to silver layer."""
    # Define schema
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    # Read source data
    customer_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(customer_schema)
        .load(CUSTOMER_DATA_PATH)
    )
    
    # Clean data
    customer_df_clean = (
        customer_df
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
        .dropDuplicates(["CustId"])
    )
    
    # Write to Delta table
    customer_df_clean.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{CUSTOMER_TABLE}")
    
    return customer_df_clean

def process_order_data(spark):
    """Process order data from source to silver layer."""
    # Define schema
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    # Read source data
    order_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_schema)
        .load(ORDER_DATA_PATH)
    )
    
    # Clean data and add TotalAmount
    order_df_clean = (
        order_df
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
        .dropDuplicates(["OrderId"])
    )
    
    # Write to Delta table
    order_df_clean.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{ORDER_TABLE}")
    
    return order_df_clean

def create_order_summary_scd2(spark, customer_df, order_df):
    """Create or update the SCD Type 2 order summary table."""
    # Check if the table exists
    table_exists = spark._jsparkSession.catalog().tableExists(TARGET_CATALOG, TARGET_SCHEMA, ORDER_SUMMARY_TABLE)
    
    # Join customer and order data
    joined_df = (
        order_df
        .join(customer_df, "CustId", "inner")
        .select(
            customer_df["CustId"],
            customer_df["Name"],
            customer_df["EmailId"],
            customer_df["Region"],
            order_df["OrderId"],
            order_df["ItemName"],
            order_df["PricePerUnit"],
            order_df["Qty"],
            order_df["Date"],
            order_df["TotalAmount"]
        )
    )
    
    current_timestamp = F.current_timestamp()
    
    if not table_exists:
        # Initialize table with all records as active
        result_df = (
            joined_df
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", current_timestamp)
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
        
        # Create the table
        result_df.write.format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .mode("overwrite") \
            .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{ORDER_SUMMARY_TABLE}")
    else:
        # Implement SCD Type 2 logic for updates
        existing_data = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{ORDER_SUMMARY_TABLE}")
        
        # Identify new and changed records
        existing_customer_attrs = existing_data.filter(F.col("IsActive") == True).select(
            "CustId", "Name", "EmailId", "Region"
        ).distinct()
        
        current_customer_attrs = customer_df.select(
            "CustId", "Name", "EmailId", "Region"
        ).distinct()
        
        # Find changed customer records
        changed_customers = (
            current_customer_attrs
            .join(existing_customer_attrs, "CustId", "inner")
            .where(
                (current_customer_attrs["Name"] != existing_customer_attrs["Name"]) |
                (current_customer_attrs["EmailId"] != existing_customer_attrs["EmailId"]) |
                (current_customer_attrs["Region"] != existing_customer_attrs["Region"])
            )
            .select(current_customer_attrs["CustId"])
            .distinct()
        )
        
        # Expire old records
        records_to_expire = (
            existing_data
            .join(changed_customers, "CustId", "inner")
            .where("IsActive = true")
            .withColumn("IsActive", F.lit(False))
            .withColumn("EndDate", current_timestamp)
        )
        
        # Create new active records for changed customers
        new_active_records = (
            joined_df
            .join(changed_customers, "CustId", "inner")
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", current_timestamp)
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
        
        # Find completely new customers
        new_customers = (
            current_customer_attrs
            .join(existing_customer_attrs, "CustId", "left_anti")
            .select("CustId")
        )
        
        # Create records for new customers
        new_customer_records = (
            joined_df
            .join(new_customers, "CustId", "inner")
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", current_timestamp)
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
        
        # Combine all records
        unchanged_records = (
            existing_data
            .join(changed_customers, "CustId", "left_anti")
        )
        
        result_df = (
            unchanged_records
            .unionByName(records_to_expire)
            .unionByName(new_active_records)
            .unionByName(new_customer_records)
        )
        
        # Update the table
        result_df.write.format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{ORDER_SUMMARY_TABLE}")
    
    return result_df

def create_customer_aggregate_spend(spark):
    """Create the customer aggregate spend table."""
    # Read from the order summary table, only active records
    order_summary_df = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{ORDER_SUMMARY_TABLE}") \
        .filter("IsActive = true")
    
    # Aggregate total amount by customer name and date
    aggregated_df = (
        order_summary_df
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )
    
    # Write to Delta table
    aggregated_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{CUSTOMER_AGGREGATE_SPEND_TABLE}")
    
    return aggregated_df

def run_pipeline():
    """Run the entire batch processing pipeline."""
    spark = create_spark_session()
    
    # Process customer and order data
    customer_df = process_customer_data(spark)
    order_df = process_order_data(spark)
    
    # Create or update SCD Type 2 order summary
    order_summary_df = create_order_summary_scd2(spark, customer_df, order_df)
    
    # Create customer aggregate spend
    customer_aggregate_df = create_customer_aggregate_spend(spark)
    
    return {
        "customer": customer_df,
        "order": order_df,
        "order_summary": order_summary_df,
        "customer_aggregate": customer_aggregate_df
    }

if __name__ == "__main__":
    run_pipeline()