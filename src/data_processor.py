from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when, datediff, row_number, sum as sum_
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
from typing import Tuple
import datetime

def get_spark_session() -> SparkSession:
    """
    Get or create a Spark session
    """
    return SparkSession.builder.appName("CustomerOrderDataProcessor").getOrCreate()

def read_source_data() -> Tuple[DataFrame, DataFrame]:
    """
    Read customer and order data from source volumes
    """
    spark = get_spark_session()
    
    # Define schemas for better control and performance
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
    
    # Read customer data
    customer_df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(customer_schema) \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    
    # Read order data
    order_df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(order_schema) \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    
    return customer_df, order_df

def clean_data(df: DataFrame) -> DataFrame:
    """
    Remove null and duplicate records from dataframe
    """
    # Drop rows with any null values
    df_no_nulls = df.na.drop()
    
    # Drop rows with string "Null" values
    for column in df.columns:
        df_no_nulls = df_no_nulls.filter(~col(column).eqNullSafe("Null"))
    
    # Drop duplicate rows
    df_clean = df_no_nulls.dropDuplicates()
    
    return df_clean

def create_ordersummary_table():
    """
    Create ordersummary table if it doesn't exist
    """
    spark = get_spark_session()
    
    # Define schema for ordersummary table
    schema = """
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
    """
    
    # Create table if not exists
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary (
        {schema}
    ) USING DELTA
    """)

def create_customeraggregatespend_table():
    """
    Create customeraggregatespend table if it doesn't exist
    """
    spark = get_spark_session()
    
    # Create table if not exists
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    ) USING DELTA
    """)

def load_scd_type2_data(customer_df: DataFrame, order_df: DataFrame):
    """
    Join customer and order data and load into SCD type 2 table
    """
    spark = get_spark_session()
    
    # Join customer and order data
    joined_df = customer_df.join(order_df, "CustId", "inner")
    
    # Check if the ordersummary table exists and has data
    try:
        existing_data = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        has_existing_data = existing_data.count() > 0
    except:
        has_existing_data = False
    
    if not has_existing_data:
        # Initial load - add SCD Type 2 columns
        current_time = current_timestamp()
        scd_df = joined_df.select(
            "CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", 
            "PricePerUnit", "Qty", "Date",
            lit(True).alias("IsActive"),
            current_time.alias("StartDate"),
            lit(None).cast(TimestampType()).alias("EndDate")
        )
        
        # Write to ordersummary table
        scd_df.write.format("delta").mode("overwrite") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    else:
        # Get current data
        current_data = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
        # Identify changes in customer data
        current_customer = current_data.select("CustId", "Name", "EmailId", "Region").distinct()
        new_customer = customer_df.select("CustId", "Name", "EmailId", "Region").distinct()
        
        # Find changed records
        changed_customers = new_customer.join(
            current_customer,
            on="CustId",
            how="left_anti"
        )
        
        if changed_customers.count() > 0:
            # Mark existing records as inactive
            current_time = current_timestamp()
            
            # Get CustIds that have changed
            changed_cust_ids = changed_customers.select("CustId").distinct()
            
            # Update existing records (set EndDate and IsActive=False)
            updated_existing = current_data.join(
                changed_cust_ids,
                on="CustId",
                how="left_outer"
            ).withColumn(
                "IsActive", 
                when(changed_cust_ids["CustId"].isNotNull(), lit(False)).otherwise(col("IsActive"))
            ).withColumn(
                "EndDate",
                when(changed_cust_ids["CustId"].isNotNull(), current_time).otherwise(col("EndDate"))
            )
            
            # Create new records for changed customers
            new_records = joined_df.join(
                changed_cust_ids,
                on="CustId",
                how="inner"
            ).select(
                joined_df["CustId"], joined_df["Name"], joined_df["EmailId"], joined_df["Region"], 
                joined_df["OrderId"], joined_df["ItemName"], joined_df["PricePerUnit"], 
                joined_df["Qty"], joined_df["Date"],
                lit(True).alias("IsActive"),
                current_time.alias("StartDate"),
                lit(None).cast(TimestampType()).alias("EndDate")
            )
            
            # Combine updated existing records with new records
            final_df = updated_existing.union(new_records)
            
            # Write back to table
            final_df.write.format("delta").mode("overwrite") \
                .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

def aggregate_customer_spend():
    """
    Aggregate customer spend and load into customeraggregatespend table
    """
    spark = get_spark_session()
    
    # Read from ordersummary table
    ordersummary_df = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    # Calculate total amount (PricePerUnit * Qty)
    ordersummary_with_total = ordersummary_df.withColumn(
        "TotalAmount", 
        col("PricePerUnit") * col("Qty")
    )
    
    # Aggregate by Name and Date
    aggregated_df = ordersummary_with_total.groupBy("Name", "Date") \
        .agg(sum_("TotalAmount").alias("TotalAmount"))
    
    # Write to customeraggregatespend table
    aggregated_df.write.format("delta").mode("overwrite") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def main():
    """
    Main execution function
    """
    # Read source data
    customer_df, order_df = read_source_data()
    
    # Clean data
    customer_clean = clean_data(customer_df)
    order_clean = clean_data(order_df)
    
    # Create tables if they don't exist
    create_ordersummary_table()
    create_customeraggregatespend_table()
    
    # Load data into SCD Type 2 table
    load_scd_type2_data(customer_clean, order_clean)
    
    # Aggregate customer spend
    aggregate_customer_spend()
    
    print("Data processing completed successfully!")

if __name__ == "__main__":
    main()