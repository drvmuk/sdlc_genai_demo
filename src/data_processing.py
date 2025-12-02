from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, sum as sum_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from delta.tables import DeltaTable
import datetime

def get_spark_session():
    """Create and return a SparkSession."""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .getOrCreate()

def read_source_data(spark):
    """
    Read source CSV data from volumes and load to Delta tables.
    
    Args:
        spark: SparkSession object
        
    Returns:
        tuple: (customer_df, order_df)
    """
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

def clean_data(customer_df, order_df):
    """
    Clean customer and order data by removing null and duplicate records.
    
    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame
        
    Returns:
        tuple: (cleaned_customer_df, cleaned_order_df)
    """
    # Clean customer data - remove nulls and duplicates
    cleaned_customer_df = customer_df.dropDuplicates(["CustId"]) \
        .filter(col("CustId").isNotNull() & 
                col("Name").isNotNull() & 
                col("EmailId").isNotNull() & 
                col("Region").isNotNull())
    
    # Clean order data - remove nulls and duplicates
    # Add TotalAmount column (PricePerUnit * Qty)
    cleaned_order_df = order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty")) \
        .dropDuplicates(["OrderId"]) \
        .filter(col("OrderId").isNotNull() & 
                col("ItemName").isNotNull() & 
                col("PricePerUnit").isNotNull() & 
                col("Qty").isNotNull() & 
                col("Date").isNotNull() & 
                col("CustId").isNotNull())
    
    return cleaned_customer_df, cleaned_order_df

def create_order_summary_table(spark, cleaned_customer_df, cleaned_order_df):
    """
    Create and update the order summary table using SCD Type 2.
    
    Args:
        spark: SparkSession object
        cleaned_customer_df: Cleaned customer DataFrame
        cleaned_order_df: Cleaned order DataFrame
    """
    # Create catalog and schema if they don't exist
    spark.sql("CREATE CATALOG IF NOT EXISTS gen_ai_poc_databrickscoe")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard")
    
    # Join customer and order data
    joined_df = cleaned_customer_df.join(
        cleaned_order_df,
        on="CustId",
        how="inner"
    ).select(
        cleaned_customer_df["CustId"],
        cleaned_customer_df["Name"],
        cleaned_customer_df["EmailId"],
        cleaned_customer_df["Region"],
        cleaned_order_df["OrderId"],
        cleaned_order_df["ItemName"],
        cleaned_order_df["PricePerUnit"],
        cleaned_order_df["Qty"],
        cleaned_order_df["Date"],
        cleaned_order_df["TotalAmount"]
    )
    
    # Check if the order summary table exists
    table_exists = spark._jsparkSession.catalog().tableExists("gen_ai_poc_databrickscoe", "sdlc_wizard", "ordersummary")
    
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    if not table_exists:
        # If table doesn't exist, create it with SCD Type 2 columns
        joined_df = joined_df.withColumn("IsActive", lit(True)) \
            .withColumn("StartDate", lit(current_date).cast("date")) \
            .withColumn("EndDate", lit(None).cast("date"))
        
        joined_df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    else:
        # If table exists, apply SCD Type 2 logic
        update_order_summary_scd2(spark, joined_df)

def update_order_summary_scd2(spark, new_data_df):
    """
    Update the order summary table using SCD Type 2 logic.
    
    Args:
        spark: SparkSession object
        new_data_df: New data to be merged into the order summary table
    """
    # Add SCD Type 2 columns to the new data
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    new_data_with_scd = new_data_df.withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", lit(current_date).cast("date")) \
        .withColumn("EndDate", lit(None).cast("date"))
    
    # Get the existing order summary table as a DeltaTable
    order_summary_table = DeltaTable.forName(spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    # Merge the new data into the existing table
    order_summary_table.alias("target").merge(
        new_data_with_scd.alias("source"),
        """target.CustId = source.CustId AND 
           target.OrderId = source.OrderId AND 
           target.IsActive = true AND
           (target.Name != source.Name OR 
            target.EmailId != source.EmailId OR 
            target.Region != source.Region OR
            target.ItemName != source.ItemName OR
            target.PricePerUnit != source.PricePerUnit OR
            target.Qty != source.Qty OR
            target.Date != source.Date OR
            target.TotalAmount != source.TotalAmount)"""
    ).whenMatchedUpdate(
        set={
            "IsActive": "false",
            "EndDate": lit(current_date).cast("date")
        }
    ).whenNotMatchedInsert(
        values={
            "CustId": "source.CustId",
            "Name": "source.Name",
            "EmailId": "source.EmailId",
            "Region": "source.Region",
            "OrderId": "source.OrderId",
            "ItemName": "source.ItemName",
            "PricePerUnit": "source.PricePerUnit",
            "Qty": "source.Qty",
            "Date": "source.Date",
            "TotalAmount": "source.TotalAmount",
            "IsActive": "true",
            "StartDate": lit(current_date).cast("date"),
            "EndDate": "null"
        }
    ).execute()
    
    # Insert new versions of updated records
    updated_records = spark.sql("""
        SELECT source.*
        FROM gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary target
        JOIN (
            SELECT * FROM gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary 
            WHERE EndDate = CAST('""" + current_date + """' AS DATE)
        ) source
        ON target.CustId = source.CustId AND target.OrderId = source.OrderId
        WHERE target.EndDate = CAST('""" + current_date + """' AS DATE)
    """)
    
    if updated_records.count() > 0:
        new_versions = new_data_with_scd.join(
            updated_records.select("CustId", "OrderId"),
            on=["CustId", "OrderId"],
            how="inner"
        )
        
        new_versions.write.format("delta") \
            .mode("append") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

def create_customer_aggregate_spend(spark):
    """
    Create the customer aggregate spend table.
    
    Args:
        spark: SparkSession object
    """
    # Create the table if it doesn't exist
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
            Name STRING,
            TotalAmount DOUBLE,
            Date DATE
        )
        USING DELTA
    """)
    
    # Aggregate data from the order summary table
    aggregate_df = spark.sql("""
        SELECT 
            Name,
            SUM(TotalAmount) AS TotalAmount,
            Date
        FROM gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary
        WHERE IsActive = true
        GROUP BY Name, Date
    """)
    
    # Write the aggregated data to the customer aggregate spend table
    aggregate_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def main():
    """Main function to execute the data processing workflow."""
    spark = get_spark_session()
    
    # Read source data
    customer_df, order_df = read_source_data(spark)
    
    # Clean data
    cleaned_customer_df, cleaned_order_df = clean_data(customer_df, order_df)
    
    # Create and update the order summary table
    create_order_summary_table(spark, cleaned_customer_df, cleaned_order_df)
    
    # Create the customer aggregate spend table
    create_customer_aggregate_spend(spark)
    
    print("Data processing completed successfully.")

if __name__ == "__main__":
    main()