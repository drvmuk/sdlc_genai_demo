from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
from delta.tables import DeltaTable
from typing import Tuple

def create_spark_session() -> SparkSession:
    """Create and return a SparkSession."""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .getOrCreate()

def read_source_data(spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    """
    Read customer and order data from source volumes.
    
    Args:
        spark: SparkSession object
        
    Returns:
        Tuple of DataFrames (customer_df, order_df)
    """
    # Read customer data
    customer_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    
    # Read order data
    order_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    
    return customer_df, order_df

def clean_data(customer_df: DataFrame, order_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Clean customer and order data by removing nulls and duplicates.
    
    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame
        
    Returns:
        Tuple of cleaned DataFrames (customer_df, order_df)
    """
    # Clean customer data
    customer_df = customer_df.na.drop() \
        .dropDuplicates(["CustId"])
    
    # Clean order data
    order_df = order_df.na.drop() \
        .dropDuplicates(["OrderId"])
    
    # Add TotalAmount column to order data
    order_df = order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    return customer_df, order_df

def save_to_delta(df: DataFrame, catalog: str, schema: str, table: str) -> None:
    """
    Save DataFrame to Delta table.
    
    Args:
        df: DataFrame to save
        catalog: Catalog name
        schema: Schema name
        table: Table name
    """
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.{table}")

def create_order_summary_table(spark: SparkSession, customer_df: DataFrame, order_df: DataFrame) -> None:
    """
    Create and populate the ordersummary table with SCD Type 2 implementation.
    
    Args:
        spark: SparkSession object
        customer_df: Customer DataFrame
        order_df: Order DataFrame
    """
    # Check if the table exists
    table_exists = False
    try:
        spark.sql(f"SELECT 1 FROM gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary LIMIT 1")
        table_exists = True
    except:
        pass
    
    # Join customer and order data
    joined_df = customer_df.join(order_df, "CustId", "inner") \
        .select(
            "CustId", 
            "Name", 
            "EmailId", 
            "Region", 
            "OrderId", 
            "ItemName", 
            "PricePerUnit", 
            "Qty", 
            "Date"
        )
    
    if not table_exists:
        # First-time creation with SCD Type 2 fields
        scd_df = joined_df \
            .withColumn("IsActive", lit(True)) \
            .withColumn("StartDate", current_timestamp()) \
            .withColumn("EndDate", lit(None).cast(TimestampType()))
        
        # Create the table
        save_to_delta(scd_df, "gen_ai_poc_databrickscoe", "sdlc_wizard", "ordersummary")
    else:
        # Update the existing SCD Type 2 table
        update_scd_type2_table(spark, joined_df)

def update_scd_type2_table(spark: SparkSession, new_data: DataFrame) -> None:
    """
    Update the SCD Type 2 table with new data.
    
    Args:
        spark: SparkSession object
        new_data: New data to be merged into the SCD Type 2 table
    """
    # Get the current table
    delta_table = DeltaTable.forName(spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    # Prepare the new data with SCD Type 2 fields
    new_data = new_data \
        .withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", current_timestamp()) \
        .withColumn("EndDate", lit(None).cast(TimestampType()))
    
    # Perform the merge operation
    delta_table.alias("target").merge(
        new_data.alias("source"),
        """
        target.CustId = source.CustId AND 
        target.OrderId = source.OrderId AND
        target.IsActive = true AND
        (
            target.Name != source.Name OR
            target.EmailId != source.EmailId OR
            target.Region != source.Region OR
            target.ItemName != source.ItemName OR
            target.PricePerUnit != source.PricePerUnit OR
            target.Qty != source.Qty OR
            target.Date != source.Date
        )
        """
    ) \
    .whenMatchedUpdate(
        set={
            "IsActive": lit(False),
            "EndDate": current_timestamp()
        }
    ) \
    .whenNotMatchedInsertAll() \
    .execute()
    
    # Insert new records for the updated ones
    current_data = delta_table.toDF()
    
    # Get records that were just expired
    expired_records = current_data.filter(
        (col("EndDate") == current_timestamp()) & 
        (col("IsActive") == False)
    )
    
    if expired_records.count() > 0:
        # Create new active records
        new_active_records = expired_records \
            .join(new_data, ["CustId", "OrderId"], "inner") \
            .select(
                new_data["CustId"],
                new_data["Name"],
                new_data["EmailId"],
                new_data["Region"],
                new_data["OrderId"],
                new_data["ItemName"],
                new_data["PricePerUnit"],
                new_data["Qty"],
                new_data["Date"],
                lit(True).alias("IsActive"),
                current_timestamp().alias("StartDate"),
                lit(None).cast(TimestampType()).alias("EndDate")
            )
        
        # Append the new records
        new_active_records.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

def create_customer_aggregate_spend(spark: SparkSession) -> None:
    """
    Create and populate the customeraggregatespend table.
    
    Args:
        spark: SparkSession object
    """
    # Check if the table exists
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
                Name STRING,
                TotalAmount DOUBLE,
                Date DATE
            )
            USING DELTA
        """)
    except Exception as e:
        print(f"Error creating table: {e}")
    
    # Get the order summary data
    order_summary_df = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    # Calculate the total amount per customer per date
    # First add the TotalAmount column if it doesn't exist
    if "TotalAmount" not in order_summary_df.columns:
        order_summary_df = order_summary_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    # Aggregate the data
    aggregated_df = order_summary_df \
        .filter(col("IsActive") == True) \
        .groupBy("Name", "Date") \
        .agg(spark_sum("TotalAmount").alias("TotalAmount"))
    
    # Save to the target table
    aggregated_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def main():
    """Main execution function."""
    spark = create_spark_session()
    
    # Read source data
    customer_df, order_df = read_source_data(spark)
    
    # Clean data
    customer_df, order_df = clean_data(customer_df, order_df)
    
    # Save to delta tables
    save_to_delta(customer_df, "gen_ai_poc_databrickscoe", "sdlc_wizard", "customer")
    save_to_delta(order_df, "gen_ai_poc_databrickscoe", "sdlc_wizard", "order")
    
    # Create order summary table
    create_order_summary_table(spark, customer_df, order_df)
    
    # Create customer aggregate spend table
    create_customer_aggregate_spend(spark)

if __name__ == "__main__":
    main()