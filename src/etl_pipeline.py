from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import datetime
from src.scd_helper import apply_scd_type2_changes

def create_spark_session():
    """Create and return a SparkSession."""
    return SparkSession.builder \
        .appName("Customer Order ETL Pipeline") \
        .getOrCreate()

def read_customer_data(spark):
    """Read customer data from volume."""
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    return spark.read \
        .option("header", "true") \
        .schema(customer_schema) \
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")

def read_order_data(spark):
    """Read order data from volume."""
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    return spark.read \
        .option("header", "true") \
        .schema(order_schema) \
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")

def clean_data(df):
    """Remove null and duplicate records."""
    # Remove rows with any null values
    df_no_nulls = df.dropna()
    
    # Remove duplicate records
    df_no_dups = df_no_nulls.dropDuplicates()
    
    return df_no_dups

def process_customer_data(spark):
    """Process customer data and save to Delta table."""
    customer_df = read_customer_data(spark)
    customer_df_clean = clean_data(customer_df)
    
    # Write to Delta table
    customer_df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customer")
    
    return customer_df_clean

def process_order_data(spark):
    """Process order data and save to Delta table."""
    order_df = read_order_data(spark)
    
    # Add TotalAmount column
    order_df = order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    order_df_clean = clean_data(order_df)
    
    # Write to Delta table
    order_df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.order")
    
    return order_df_clean

def create_order_summary(spark, customer_df, order_df):
    """Create order summary table with SCD Type 2."""
    # Create the ordersummary table if it doesn't exist
    spark.sql("""
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
        TotalAmount DOUBLE,
        IsActive BOOLEAN,
        StartDate TIMESTAMP,
        EndDate TIMESTAMP
    ) USING DELTA
    """)
    
    # Join customer and order data
    joined_df = order_df.join(
        customer_df,
        on="CustId",
        how="inner"
    ).select(
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
    
    # Get existing data from ordersummary
    try:
        current_order_summary = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
        # Apply SCD Type 2 changes
        updated_order_summary = apply_scd_type2_changes(
            current_df=current_order_summary,
            new_df=joined_df,
            key_columns=["CustId", "OrderId"],
            change_columns=["Name", "EmailId", "Region", "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"]
        )
        
        # Write updated data back to the table
        updated_order_summary.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    except:
        # First run - initialize with active records
        initial_df = joined_df.withColumn("IsActive", lit(True)) \
            .withColumn("StartDate", current_timestamp()) \
            .withColumn("EndDate", lit(None).cast("timestamp"))
            
        initial_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

def create_customer_aggregate_spend(spark):
    """Create customer aggregate spend table."""
    # Create the table if it doesn't exist
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    ) USING DELTA
    """)
    
    # Get active records from ordersummary
    order_summary_df = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary") \
        .filter(col("IsActive") == True)
    
    # Aggregate TotalAmount by Name and Date
    aggregated_df = order_summary_df.groupBy("Name", "Date") \
        .agg(spark_sum("TotalAmount").alias("TotalAmount"))
    
    # Write to customeraggregatespend table
    aggregated_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def run_etl_pipeline():
    """Run the complete ETL pipeline."""
    spark = create_spark_session()
    
    # Process customer and order data
    customer_df = process_customer_data(spark)
    order_df = process_order_data(spark)
    
    # Create order summary with SCD Type 2
    create_order_summary(spark, customer_df, order_df)
    
    # Create customer aggregate spend
    create_customer_aggregate_spend(spark)
    
    print("ETL pipeline completed successfully.")

if __name__ == "__main__":
    run_etl_pipeline()