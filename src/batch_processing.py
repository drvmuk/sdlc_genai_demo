from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

def create_spark_session():
    """Create and return a Spark session"""
    return (SparkSession.builder
            .appName("Customer Order Processing")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def read_customer_data(spark):
    """Read customer data from volume"""
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    return (spark.read.format("csv")
            .option("header", "true")
            .schema(customer_schema)
            .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"))

def read_order_data(spark):
    """Read order data from volume"""
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    return (spark.read.format("csv")
            .option("header", "true")
            .schema(order_schema)
            .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"))

def clean_customer_data(customer_df):
    """Clean customer data by removing nulls and duplicates"""
    return (customer_df
            .filter(
                (F.col("CustId").isNotNull()) &
                (F.col("Name").isNotNull()) &
                (F.col("EmailId").isNotNull()) &
                (F.col("Region").isNotNull())
            )
            .dropDuplicates(["CustId"]))

def clean_order_data(order_df):
    """Clean order data and add TotalAmount column"""
    return (order_df
            .filter(
                (F.col("OrderId").isNotNull()) &
                (F.col("ItemName").isNotNull()) &
                (F.col("PricePerUnit").isNotNull()) &
                (F.col("Qty").isNotNull()) &
                (F.col("Date").isNotNull()) &
                (F.col("CustId").isNotNull())
            )
            .dropDuplicates(["OrderId"])
            .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty")))

def create_tables_if_not_exists(spark):
    """Create required tables if they don't exist"""
    # Create ordersummary table if not exists
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
    PARTITIONED BY (Date)
    """)
    
    # Create customeraggregatespend table if not exists
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    )
    USING DELTA
    PARTITIONED BY (Date)
    """)

def update_scd_type2_table(spark, customer_df, order_df):
    """Update the SCD Type 2 ordersummary table"""
    # Join customer and order data
    joined_data = (customer_df
                  .join(order_df, on="CustId", how="inner")
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
                  ))
    
    # Check if table is empty
    count = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary").count()
    
    if count == 0:
        # Initial load
        (joined_data
         .withColumn("IsActive", F.lit(True))
         .withColumn("StartDate", F.current_timestamp())
         .withColumn("EndDate", F.lit(None).cast(TimestampType()))
         .write
         .format("delta")
         .mode("overwrite")
         .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"))
        return
    
    # Get the current table as a DeltaTable
    delta_table = DeltaTable.forName(spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    # Prepare the new data with SCD Type 2 attributes
    new_data = (joined_data
               .withColumn("IsActive", F.lit(True))
               .withColumn("StartDate", F.current_timestamp())
               .withColumn("EndDate", F.lit(None).cast(TimestampType())))
    
    # Identify records that need to be updated (matched on CustId and OrderId)
    matched_updates = (
        delta_table.toDF().alias("current")
        .join(
            new_data.alias("updates"),
            (F.col("current.CustId") == F.col("updates.CustId")) &
            (F.col("current.OrderId") == F.col("updates.OrderId")) &
            (F.col("current.IsActive") == True),
            "inner"
        )
        .where(
            (F.col("current.Name") != F.col("updates.Name")) |
            (F.col("current.EmailId") != F.col("updates.EmailId")) |
            (F.col("current.Region") != F.col("updates.Region"))
        )
    )
    
    # If there are updates, perform SCD Type 2 operations
    if matched_updates.count() > 0:
        # Update the existing records (mark as inactive)
        delta_table.alias("current").merge(
            matched_updates.select("current.*").alias("updates"),
            "current.CustId = updates.CustId AND current.OrderId = updates.OrderId AND current.IsActive = true"
        ).whenMatched().updateExpr({
            "IsActive": "false",
            "EndDate": "current_timestamp()"
        }).execute()
        
        # Insert the new versions of updated records
        new_records = (
            matched_updates.select("updates.*")
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", F.current_timestamp())
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        )
        
        new_records.write.format("delta").mode("append").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    # Insert completely new records (not existing in the target table)
    new_inserts = (
        new_data.alias("new")
        .join(
            delta_table.toDF().select("CustId", "OrderId").distinct().alias("existing"),
            (F.col("new.CustId") == F.col("existing.CustId")) &
            (F.col("new.OrderId") == F.col("existing.OrderId")),
            "left_anti"
        )
    )
    
    if new_inserts.count() > 0:
        new_inserts.write.format("delta").mode("append").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

def update_customer_aggregate_spend(spark):
    """Update the customeraggregatespend table with aggregated data"""
    # Aggregate data from ordersummary
    aggregated_data = (
        spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        .filter(F.col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )
    
    # Write to customeraggregatespend table
    aggregated_data.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def main():
    """Main function to orchestrate the data processing pipeline"""
    spark = create_spark_session()
    
    # Read source data
    customer_df = read_customer_data(spark)
    order_df = read_order_data(spark)
    
    # Clean data
    customer_df_clean = clean_customer_data(customer_df)
    order_df_clean = clean_order_data(order_df)
    
    # Create tables if not exists
    create_tables_if_not_exists(spark)
    
    # Update SCD Type 2 table
    update_scd_type2_table(spark, customer_df_clean, order_df_clean)
    
    # Update aggregate spend table
    update_customer_aggregate_spend(spark)
    
    print("Data processing completed successfully!")

if __name__ == "__main__":
    main()