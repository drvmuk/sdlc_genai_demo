from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

def create_spark_session():
    """Create a Spark session for batch processing."""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .enableHiveSupport() \
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
        .format("csv") \
        .option("header", "true") \
        .schema(customer_schema) \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")

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
        .format("csv") \
        .option("header", "true") \
        .schema(order_schema) \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")

def clean_customer_data(customer_df):
    """Clean customer data by removing nulls and duplicates."""
    return customer_df \
        .dropDuplicates(["CustId"]) \
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull()) &
            (F.col("CustId") != "Null") &
            (F.col("Name") != "Null") &
            (F.col("EmailId") != "Null") &
            (F.col("Region") != "Null")
        )

def clean_order_data(order_df):
    """Clean order data and add TotalAmount column."""
    return order_df \
        .dropDuplicates(["OrderId"]) \
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull()) &
            (F.col("OrderId") != "Null") &
            (F.col("ItemName") != "Null") &
            (F.col("CustId") != "Null")
        ) \
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))

def save_to_delta_table(df, table_name, mode="overwrite"):
    """Save DataFrame to Delta table."""
    df.write \
        .format("delta") \
        .mode(mode) \
        .saveAsTable(table_name)

def create_or_update_scd_type2_table(spark, customer_df, order_df):
    """Create or update SCD Type 2 table for ordersummary."""
    # Check if table exists
    table_exists = spark._jsparkSession.catalog().tableExists("gen_ai_poc_databrickscoe", "sdlc_wizard", "ordersummary")
    
    # Join customer and order data
    joined_data = customer_df \
        .join(
            order_df,
            "CustId",
            "inner"
        ) \
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
    
    if not table_exists:
        # First-time creation
        joined_data \
            .withColumn("IsActive", F.lit(True)) \
            .withColumn("StartDate", F.current_timestamp()) \
            .withColumn("EndDate", F.lit(None).cast(TimestampType())) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    else:
        # SCD Type 2 update logic
        delta_table = DeltaTable.forName(spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        current_data = delta_table.toDF()
        
        # Find active records
        active_records = current_data.filter(F.col("IsActive") == True)
        
        # Find changed records
        changed_records = joined_data \
            .join(
                active_records,
                ["CustId", "OrderId"],
                "inner"
            ) \
            .filter(
                (joined_data.Name != active_records.Name) |
                (joined_data.EmailId != active_records.EmailId) |
                (joined_data.Region != active_records.Region)
            ) \
            .select(active_records.CustId, active_records.OrderId)
        
        # Expire changed records
        if changed_records.count() > 0:
            delta_table.update(
                condition=(F.col("CustId").isin([r.CustId for r in changed_records.collect()]) & 
                          F.col("OrderId").isin([r.OrderId for r in changed_records.collect()]) &
                          F.col("IsActive") == True),
                set={
                    "IsActive": F.lit(False),
                    "EndDate": F.current_timestamp()
                }
            )
            
            # Insert new versions of changed records
            new_records = joined_data \
                .join(
                    changed_records,
                    ["CustId", "OrderId"],
                    "inner"
                ) \
                .withColumn("IsActive", F.lit(True)) \
                .withColumn("StartDate", F.current_timestamp()) \
                .withColumn("EndDate", F.lit(None).cast(TimestampType()))
            
            new_records.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
        # Insert completely new records
        new_records = joined_data \
            .join(
                active_records.select("CustId", "OrderId"),
                ["CustId", "OrderId"],
                "left_anti"
            ) \
            .withColumn("IsActive", F.lit(True)) \
            .withColumn("StartDate", F.current_timestamp()) \
            .withColumn("EndDate", F.lit(None).cast(TimestampType()))
        
        if new_records.count() > 0:
            new_records.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

def create_customer_aggregate_spend(spark):
    """Create customer aggregate spend table."""
    # Create the table if it doesn't exist
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    )
    USING DELTA
    """)
    
    # Calculate aggregations from ordersummary
    agg_data = spark.sql("""
    SELECT 
        Name,
        Date,
        SUM(TotalAmount) as TotalAmount
    FROM gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary
    WHERE IsActive = true
    GROUP BY Name, Date
    """)
    
    # Write to customeraggregatespend table
    agg_data.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def main():
    """Main function to orchestrate the data processing pipeline."""
    spark = create_spark_session()
    
    # Read data
    customer_df = read_customer_data(spark)
    order_df = read_order_data(spark)
    
    # Clean data
    customer_df_clean = clean_customer_data(customer_df)
    order_df_clean = clean_order_data(order_df)
    
    # Save cleaned data to Delta tables
    save_to_delta_table(customer_df_clean, "gen_ai_poc_databrickscoe.sdlc_wizard.customer")
    save_to_delta_table(order_df_clean, "gen_ai_poc_databrickscoe.sdlc_wizard.order")
    
    # Create or update SCD Type 2 table
    create_or_update_scd_type2_table(spark, customer_df_clean, order_df_clean)
    
    # Create customer aggregate spend table
    create_customer_aggregate_spend(spark)
    
    spark.stop()

if __name__ == "__main__":
    main()