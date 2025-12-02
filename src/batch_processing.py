from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def read_source_data(spark):
    """Read source data from volumes."""
    customer_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    
    order_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    
    return customer_df, order_df

def clean_data(customer_df, order_df):
    """Clean data by removing nulls and duplicates."""
    # Clean customer data
    clean_customer_df = customer_df.dropDuplicates(["CustId"]) \
        .filter(F.col("CustId").isNotNull() & 
                F.col("Name").isNotNull() & 
                F.col("EmailId").isNotNull() & 
                F.col("Region").isNotNull())
    
    # Clean order data and add TotalAmount column
    clean_order_df = order_df.dropDuplicates(["OrderId"]) \
        .filter(F.col("OrderId").isNotNull() & 
                F.col("ItemName").isNotNull() & 
                F.col("PricePerUnit").isNotNull() & 
                F.col("Qty").isNotNull() & 
                F.col("Date").isNotNull() & 
                F.col("CustId").isNotNull()) \
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    
    return clean_customer_df, clean_order_df

def create_ordersummary(spark, customer_df, order_df):
    """Create or update ordersummary table with SCD Type 2 implementation."""
    # Join customer and order data
    joined_df = order_df.join(customer_df, "CustId", "inner")
    
    # Select columns as per the required schema
    current_data = joined_df.select(
        "CustId", "Name", "EmailId", "Region", 
        "OrderId", "ItemName", "PricePerUnit", "Qty", "Date"
    )
    
    # Create catalog and schema if not exists
    spark.sql("CREATE CATALOG IF NOT EXISTS gen_ai_poc_databrickscoe")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard")
    
    # Check if table exists
    table_exists = spark._jsparkSession.catalog().tableExists("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    if table_exists:
        # Implement SCD Type 2 logic
        target_table = DeltaTable.forName(spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
        # Add hash to detect changes
        current_data = current_data.withColumn(
            "row_hash", 
            F.sha2(
                F.concat_ws("||", 
                           F.col("Name"), 
                           F.col("EmailId"), 
                           F.col("Region"),
                           F.col("ItemName"),
                           F.col("PricePerUnit"),
                           F.col("Qty"),
                           F.col("Date")), 
                256
            )
        )
        
        # Add current timestamp for new records
        current_data = current_data.withColumn("StartDate", F.current_timestamp())
        current_data = current_data.withColumn("EndDate", F.lit(None).cast("timestamp"))
        current_data = current_data.withColumn("IsActive", F.lit(True))
        
        # Convert target to DataFrame for comparison
        target_df = target_table.toDF()
        
        # Add row_hash to target for comparison
        target_with_hash = target_df.withColumn(
            "row_hash", 
            F.sha2(
                F.concat_ws("||", 
                           F.col("Name"), 
                           F.col("EmailId"), 
                           F.col("Region"),
                           F.col("ItemName"),
                           F.col("PricePerUnit"),
                           F.col("Qty"),
                           F.col("Date")), 
                256
            )
        )
        
        # Find records that changed
        join_condition = (current_data["CustId"] == target_with_hash["CustId"]) & \
                         (current_data["OrderId"] == target_with_hash["OrderId"])
                         
        # Records that exist in source but have different hash in target
        changed_records = current_data.join(
            target_with_hash, 
            join_condition & (current_data["row_hash"] != target_with_hash["row_hash"]) & target_with_hash["IsActive"],
            "inner"
        ).select(current_data["*"])
        
        # Records that exist in source but not in target
        new_records = current_data.join(
            target_with_hash,
            join_condition,
            "left_anti"
        )
        
        # Combine new and changed records
        updates = changed_records.union(new_records).drop("row_hash")
        
        # Update the target table - expire old records and insert new ones
        target_table.alias("target").merge(
            updates.alias("updates"),
            "target.CustId = updates.CustId AND target.OrderId = updates.OrderId AND target.IsActive = true"
        ).whenMatched().updateExpr({
            "IsActive": "false",
            "EndDate": "current_timestamp()"
        }).whenNotMatched().insertAll().execute()
        
    else:
        # Create new table if it doesn't exist
        result_df = current_data \
            .withColumn("StartDate", F.current_timestamp()) \
            .withColumn("EndDate", F.lit(None).cast("timestamp")) \
            .withColumn("IsActive", F.lit(True))
            
        result_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")

def create_customeraggregatespend(spark, order_df):
    """Create customeraggregatespend table with aggregated data."""
    # Create table if not exists
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date STRING
    )
    USING DELTA
    """)
    
    # Read ordersummary table
    ordersummary_df = spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
    
    # Join with order data to get TotalAmount
    joined_df = ordersummary_df.join(
        order_df,
        ["OrderId"],
        "inner"
    )
    
    # Aggregate TotalAmount by Name and Date
    aggregated_df = joined_df.groupBy("Name", "Date") \
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    
    # Write to customeraggregatespend table
    aggregated_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend")

def main():
    """Main function to execute the data processing pipeline."""
    spark = create_spark_session()
    
    # Read source data
    customer_df, order_df = read_source_data(spark)
    
    # Clean data
    clean_customer_df, clean_order_df = clean_data(customer_df, order_df)
    
    # Create or update ordersummary table
    create_ordersummary(spark, clean_customer_df, clean_order_df)
    
    # Create customeraggregatespend table
    create_customeraggregatespend(spark, clean_order_df)
    
    print("Data processing completed successfully")

if __name__ == "__main__":
    main()