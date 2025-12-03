from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, datediff, to_date, sum as sum_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import datetime

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .enableHiveSupport() \
        .getOrCreate()

def read_customer_data(spark):
    """Read customer data from volume."""
    customer_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
    
    # Define schema for customer data
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    return spark.read.csv(customer_path, header=True, schema=customer_schema)

def read_order_data(spark):
    """Read order data from volume."""
    order_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
    
    # Define schema for order data
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    return spark.read.csv(order_path, header=True, schema=order_schema)

def clean_data(df):
    """Clean data by removing nulls and duplicates."""
    # Remove rows with any null values
    df_no_nulls = df.dropna()
    
    # Remove duplicate rows
    df_clean = df_no_nulls.dropDuplicates()
    
    return df_clean

def process_order_data(order_df):
    """Process order data by adding TotalAmount column."""
    return order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))

def create_catalog_schema(spark):
    """Create catalog and schema if they don't exist."""
    spark.sql("CREATE CATALOG IF NOT EXISTS gen_ai_poc_databrickscoe")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard")

def create_ordersummary_table(spark):
    """Create ordersummary table if it doesn't exist."""
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
    )
    USING DELTA
    """)

def create_customeraggregatespend_table(spark):
    """Create customeraggregatespend table if it doesn't exist."""
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    )
    USING DELTA
    """)

def update_scd_type2_table(spark, customer_df, order_df):
    """
    Update the SCD Type 2 table with changes from customer data.
    """
    # Get current data from ordersummary
    current_data = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    # Join customer and order data
    joined_data = customer_df.join(order_df, "CustId")
    
    # Prepare new data with SCD Type 2 columns
    new_data = joined_data.select(
        "CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", 
        "PricePerUnit", "Qty", "Date", "TotalAmount"
    ).withColumn("IsActive", lit(True)) \
     .withColumn("StartDate", current_timestamp()) \
     .withColumn("EndDate", lit(None).cast("timestamp"))
    
    # If there's no existing data, just insert the new data
    if current_data.count() == 0:
        new_data.write.format("delta").mode("overwrite") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        return
    
    # Find changed records
    # Get active records
    active_records = current_data.filter(col("IsActive") == True)
    
    # Find records that have changed (comparing customer attributes)
    changed_records = active_records.join(
        customer_df,
        (active_records.CustId == customer_df.CustId),
        "left_anti"
    )
    
    # Update existing records (mark as inactive)
    if changed_records.count() > 0:
        # Get IDs of changed records
        changed_ids = changed_records.select("CustId").distinct()
        
        # Update existing records to mark them as inactive
        spark.sql(f"""
        UPDATE gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary
        SET IsActive = FALSE, EndDate = current_timestamp()
        WHERE CustId IN (SELECT CustId FROM {changed_ids.createOrReplaceTempView("changed_ids"); "changed_ids"})
        AND IsActive = TRUE
        """)
        
        # Insert new records
        new_records = joined_data.join(
            changed_ids, 
            joined_data.CustId == changed_ids.CustId
        ).select(
            joined_data.CustId, joined_data.Name, joined_data.EmailId, joined_data.Region, 
            joined_data.OrderId, joined_data.ItemName, joined_data.PricePerUnit, 
            joined_data.Qty, joined_data.Date, joined_data.TotalAmount,
            lit(True).alias("IsActive"),
            current_timestamp().alias("StartDate"),
            lit(None).cast("timestamp").alias("EndDate")
        )
        
        new_records.write.format("delta").mode("append") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    # Insert completely new records (not in the existing data)
    new_customer_ids = customer_df.select("CustId").subtract(
        current_data.select("CustId").distinct()
    )
    
    if new_customer_ids.count() > 0:
        new_customer_records = joined_data.join(
            new_customer_ids,
            joined_data.CustId == new_customer_ids.CustId
        ).select(
            joined_data.CustId, joined_data.Name, joined_data.EmailId, joined_data.Region, 
            joined_data.OrderId, joined_data.ItemName, joined_data.PricePerUnit, 
            joined_data.Qty, joined_data.Date, joined_data.TotalAmount,
            lit(True).alias("IsActive"),
            current_timestamp().alias("StartDate"),
            lit(None).cast("timestamp").alias("EndDate")
        )
        
        new_customer_records.write.format("delta").mode("append") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

def update_customeraggregatespend(spark):
    """
    Aggregate TotalAmount by Name and Date from ordersummary and update customeraggregatespend table.
    """
    # Get active records from ordersummary
    active_records = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary") \
                         .filter(col("IsActive") == True)
    
    # Aggregate TotalAmount by Name and Date
    aggregated_data = active_records.groupBy("Name", "Date") \
                                   .agg(sum_("TotalAmount").alias("TotalAmount"))
    
    # Write to customeraggregatespend table
    aggregated_data.write.format("delta").mode("overwrite") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def main():
    """Main function to orchestrate the data processing."""
    spark = create_spark_session()
    
    # Read data from volumes
    customer_df_raw = read_customer_data(spark)
    order_df_raw = read_order_data(spark)
    
    # Clean data
    customer_df = clean_data(customer_df_raw)
    order_df_raw = clean_data(order_df_raw)
    
    # Process order data
    order_df = process_order_data(order_df_raw)
    
    # Create catalog and schema
    create_catalog_schema(spark)
    
    # Create tables if they don't exist
    create_ordersummary_table(spark)
    create_customeraggregatespend_table(spark)
    
    # Update SCD Type 2 table
    update_scd_type2_table(spark, customer_df, order_df)
    
    # Update customeraggregatespend table
    update_customeraggregatespend(spark)
    
    print("Data processing completed successfully.")

if __name__ == "__main__":
    main()