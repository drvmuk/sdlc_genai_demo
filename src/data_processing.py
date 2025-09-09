"""
Main data processing module for customer and order data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, BooleanType, TimestampType
from delta.tables import DeltaTable
import datetime

from src.config import (
    CUSTOMER_DATA_PATH, ORDER_DATA_PATH, CATALOG, SCHEMA,
    CUSTOMER_TABLE, ORDER_TABLE, ORDER_SUMMARY_TABLE, CUSTOMER_AGGREGATE_SPEND_TABLE,
    CUSTOMER_SCHEMA, ORDER_SCHEMA, ORDER_SUMMARY_SCHEMA, CUSTOMER_AGGREGATE_SCHEMA
)

def get_spark_session():
    """Create and return a SparkSession."""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .getOrCreate()

def read_csv_data(spark, path, schema=None):
    """Read CSV data from the specified path."""
    if schema:
        return spark.read.option("header", "true").schema(schema).csv(path)
    else:
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)

def clean_data(df):
    """Remove null and duplicate records from the dataframe."""
    # Drop rows with any null values
    df_no_nulls = df.na.drop()
    
    # Drop duplicate rows
    df_clean = df_no_nulls.dropDuplicates()
    
    return df_clean

def process_customer_data(spark):
    """Process customer data from source to delta table."""
    # Read customer data
    customer_df = read_csv_data(spark, CUSTOMER_DATA_PATH, CUSTOMER_SCHEMA)
    
    # Clean data
    customer_df_clean = clean_data(customer_df)
    
    # Write to delta table
    customer_df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{CUSTOMER_TABLE}")
    
    return customer_df_clean

def process_order_data(spark):
    """Process order data from source to delta table."""
    # Read order data
    order_df = read_csv_data(spark, ORDER_DATA_PATH, ORDER_SCHEMA)
    
    # Add TotalAmount column
    order_df = order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    # Clean data
    order_df_clean = clean_data(order_df)
    
    # Write to delta table
    order_df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{ORDER_TABLE}")
    
    return order_df_clean

def create_order_summary_table(spark, customer_df, order_df):
    """Create or update the order summary table using SCD Type 2."""
    # Create the table if it doesn't exist
    table_exists = spark._jsparkSession.catalog().tableExists(CATALOG, SCHEMA, ORDER_SUMMARY_TABLE)
    
    # Join customer and order data
    joined_df = order_df.join(customer_df, "CustId")
    
    # Add SCD Type 2 columns
    current_time = current_timestamp()
    joined_df = joined_df.withColumn("IsActive", lit(True)) \
                         .withColumn("StartDate", current_time) \
                         .withColumn("EndDate", lit(None).cast(TimestampType()))
    
    if not table_exists:
        # Create the table for the first time
        joined_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"{CATALOG}.{SCHEMA}.{ORDER_SUMMARY_TABLE}")
    else:
        # Update the existing table using SCD Type 2 logic
        update_order_summary_scd2(spark, joined_df)
    
    return joined_df

def update_order_summary_scd2(spark, new_data):
    """Update the order summary table using SCD Type 2 logic."""
    # Get the target Delta table
    target_table = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.{ORDER_SUMMARY_TABLE}")
    
    # Identify records that need to be updated (where customer data has changed)
    target_df = target_table.toDF()
    
    # Join with new data to find changes
    join_condition = """
        target.CustId = source.CustId AND
        target.OrderId = source.OrderId AND
        target.IsActive = true AND
        (
            target.Name != source.Name OR
            target.EmailId != source.EmailId OR
            target.Region != source.Region
        )
    """
    
    # Perform the merge operation
    target_table.alias("target").merge(
        new_data.alias("source"),
        join_condition
    ).whenMatchedUpdate(
        set={
            "IsActive": "false",
            "EndDate": current_timestamp()
        }
    ).execute()
    
    # Insert new records
    new_records = new_data.alias("source").join(
        target_df.alias("target"),
        (col("source.CustId") == col("target.CustId")) &
        (col("source.OrderId") == col("target.OrderId")) &
        col("target.IsActive"),
        "left_anti"
    )
    
    if new_records.count() > 0:
        new_records.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{CATALOG}.{SCHEMA}.{ORDER_SUMMARY_TABLE}")

def create_customer_aggregate_spend(spark):
    """Create the customer aggregate spend table."""
    # Create the table if it doesn't exist
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{CUSTOMER_AGGREGATE_SPEND_TABLE} (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    )
    USING DELTA
    """)
    
    # Aggregate data from order summary
    agg_df = spark.sql(f"""
    SELECT 
        Name,
        SUM(TotalAmount) as TotalAmount,
        Date
    FROM {CATALOG}.{SCHEMA}.{ORDER_SUMMARY_TABLE}
    WHERE IsActive = true
    GROUP BY Name, Date
    """)
    
    # Write to the target table
    agg_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{CUSTOMER_AGGREGATE_SPEND_TABLE}")
    
    return agg_df

def main():
    """Main execution function."""
    spark = get_spark_session()
    
    # Process customer and order data
    customer_df = process_customer_data(spark)
    order_df = process_order_data(spark)
    
    # Create order summary table
    create_order_summary_table(spark, customer_df, order_df)
    
    # Create customer aggregate spend table
    create_customer_aggregate_spend(spark)
    
    print("Data processing completed successfully.")

if __name__ == "__main__":
    main()