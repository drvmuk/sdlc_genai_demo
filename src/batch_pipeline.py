from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .enableHiveSupport() \
        .getOrCreate()

def read_source_data(spark):
    """Read source data from volumes."""
    # Read customer data
    customer_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    
    # Read order data
    order_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    
    return customer_df, order_df

def clean_data(customer_df, order_df):
    """Clean data by removing nulls and duplicates."""
    # Clean customer data
    clean_customer_df = customer_df \
        .dropDuplicates(["CustId"]) \
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
    
    # Clean order data and add TotalAmount column
    clean_order_df = order_df \
        .dropDuplicates(["OrderId"]) \
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        ) \
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    
    return clean_customer_df, clean_order_df

def create_or_update_ordersummary(spark, clean_customer_df, clean_order_df):
    """Create or update the ordersummary SCD Type 2 table."""
    # Get the current timestamp
    current_timestamp = F.current_timestamp()
    
    # Create catalog and schema if they don't exist
    spark.sql("CREATE CATALOG IF NOT EXISTS gen_ai_poc_databrickscoe")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard")
    
    # Check if the table exists
    table_exists = spark._jsparkSession.catalog().tableExists("gen_ai_poc_databrickscoe", "sdlc_wizard", "ordersummary")
    
    if table_exists:
        # Get existing customer data from ordersummary
        existing_customer_df = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary") \
            .filter(F.col("IsActive") == True) \
            .select("CustId", "Name", "EmailId", "Region") \
            .distinct()
        
        # Find changed customers
        changed_customers = clean_customer_df.alias("new") \
            .join(
                existing_customer_df.alias("old"),
                "CustId",
                "inner"
            ) \
            .filter(
                (F.col("new.Name") != F.col("old.Name")) |
                (F.col("new.EmailId") != F.col("old.EmailId")) |
                (F.col("new.Region") != F.col("old.Region"))
            ) \
            .select("CustId")
        
        # Expire old records
        if changed_customers.count() > 0:
            delta_table = DeltaTable.forName(spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
            delta_table.alias("t").merge(
                changed_customers.alias("s"),
                "t.CustId = s.CustId AND t.IsActive = true"
            ).whenMatchedUpdate(
                set={
                    "IsActive": "false",
                    "EndDate": "current_timestamp()"
                }
            ).execute()
        
        # Insert new records for changed customers
        new_records = clean_customer_df.alias("c") \
            .join(
                changed_customers.alias("cc"),
                "CustId",
                "inner"
            ) \
            .join(
                clean_order_df.alias("o"),
                "CustId",
                "inner"
            ) \
            .select(
                F.col("c.CustId"),
                F.col("c.Name"),
                F.col("c.EmailId"),
                F.col("c.Region"),
                F.col("o.OrderId"),
                F.col("o.ItemName"),
                F.col("o.PricePerUnit"),
                F.col("o.Qty"),
                F.col("o.Date"),
                F.col("o.TotalAmount"),
                F.lit(True).alias("IsActive"),
                current_timestamp.alias("StartDate"),
                F.lit(None).cast("timestamp").alias("EndDate")
            )
        
        # Insert new records
        if new_records.count() > 0:
            new_records.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
    else:
        # First run - create the table with all records as active
        joined_df = clean_customer_df.alias("c") \
            .join(
                clean_order_df.alias("o"),
                "CustId",
                "inner"
            ) \
            .select(
                F.col("c.CustId"),
                F.col("c.Name"),
                F.col("c.EmailId"),
                F.col("c.Region"),
                F.col("o.OrderId"),
                F.col("o.ItemName"),
                F.col("o.PricePerUnit"),
                F.col("o.Qty"),
                F.col("o.Date"),
                F.col("o.TotalAmount"),
                F.lit(True).alias("IsActive"),
                current_timestamp.alias("StartDate"),
                F.lit(None).cast("timestamp").alias("EndDate")
            )
        
        # Create the ordersummary table
        joined_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

def create_customer_aggregate_spend(spark):
    """Create the customeraggregatespend table."""
    # Create table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
            Name STRING,
            TotalAmount DOUBLE,
            Date DATE
        )
        USING DELTA
        PARTITIONED BY (Date)
    """)
    
    # Aggregate data from ordersummary
    agg_df = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary") \
        .filter(F.col("IsActive") == True) \
        .groupBy("Name", "Date") \
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    
    # Write to customeraggregatespend table
    agg_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def main():
    """Main function to orchestrate the data processing."""
    spark = create_spark_session()
    
    # Read source data
    customer_df, order_df = read_source_data(spark)
    
    # Clean data
    clean_customer_df, clean_order_df = clean_data(customer_df, order_df)
    
    # Write cleaned data to delta tables
    clean_customer_df.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customer")
    clean_order_df.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.order")
    
    # Create or update ordersummary table
    create_or_update_ordersummary(spark, clean_customer_df, clean_order_df)
    
    # Create customer aggregate spend table
    create_customer_aggregate_spend(spark)
    
    print("Data processing completed successfully.")

if __name__ == "__main__":
    main()