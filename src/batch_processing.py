from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def create_spark_session():
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .enableHiveSupport() \
        .getOrCreate()

def read_source_data(spark):
    """Read source data from volumes"""
    customer_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    
    order_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    
    return customer_df, order_df

def clean_and_transform_data(customer_df, order_df):
    """Clean data and add derived columns"""
    # Remove nulls and duplicates from customer data
    customer_clean = customer_df.filter(
        (F.col("CustId").isNotNull()) & 
        (F.col("Name").isNotNull()) & 
        (F.col("EmailId").isNotNull()) & 
        (F.col("Region").isNotNull())
    ).dropDuplicates()
    
    # Remove nulls and duplicates from order data and calculate TotalAmount
    order_clean = order_df.filter(
        (F.col("OrderId").isNotNull()) &
        (F.col("ItemName").isNotNull()) &
        (F.col("PricePerUnit").isNotNull()) &
        (F.col("Qty").isNotNull()) &
        (F.col("Date").isNotNull()) &
        (F.col("CustId").isNotNull())
    ).dropDuplicates() \
     .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    
    return customer_clean, order_clean

def create_or_update_ordersummary(spark, customer_df, order_df):
    """Create or update the ordersummary SCD Type 2 table"""
    # Create catalog and schema if they don't exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS gen_ai_poc_databrickscoe")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard")
    
    # Join customer and order data
    current_data = customer_df.join(
        order_df,
        on="CustId",
        how="inner"
    ).select(
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
    
    # Check if the table exists
    table_exists = spark._jsparkSession.catalog().tableExists("gen_ai_poc_databrickscoe", "sdlc_wizard", "ordersummary")
    
    if not table_exists:
        # First-time load - create the table with SCD Type 2 columns
        (current_data
         .withColumn("StartDate", F.current_timestamp())
         .withColumn("EndDate", F.lit(None).cast("timestamp"))
         .withColumn("IsActive", F.lit(True))
         .write
         .format("delta")
         .mode("overwrite")
         .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"))
    else:
        # Table exists, perform SCD Type 2 updates
        delta_table = DeltaTable.forName(spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        
        # Get existing data
        existing_data = delta_table.toDF()
        
        # Find the latest version of each customer record
        window_spec = Window.partitionBy("CustId").orderBy(F.desc("StartDate"))
        latest_customer_records = (
            existing_data
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .filter(F.col("IsActive") == True)
            .drop("row_num")
        )
        
        # Find changed records
        changed_records = (
            current_data.join(
                latest_customer_records.select("CustId", "Name", "EmailId", "Region"),
                on=["CustId", "Name", "EmailId", "Region"],
                how="left_anti"
            )
        )
        
        # Get the CustIds that have changed
        changed_cust_ids = changed_records.select("CustId").distinct()
        
        # Expire old records
        delta_table.update(
            condition=F.expr("""CustId IN (
                SELECT CustId FROM changed_cust_ids
            ) AND IsActive = true"""),
            set={
                "EndDate": F.current_timestamp(),
                "IsActive": F.lit(False)
            }
        )
        
        # Insert new active records
        (changed_records
         .withColumn("StartDate", F.current_timestamp())
         .withColumn("EndDate", F.lit(None).cast("timestamp"))
         .withColumn("IsActive", F.lit(True))
         .write
         .format("delta")
         .mode("append")
         .saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"))

def create_customer_aggregate_spend(spark):
    """Create and populate the customeraggregatespend table"""
    # Create the table if it doesn't exist
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date STRING
    )
    USING DELTA
    """)
    
    # Aggregate data from ordersummary and load into customeraggregatespend
    spark.sql("""
    INSERT OVERWRITE TABLE gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend
    SELECT 
        Name,
        SUM(TotalAmount) as TotalAmount,
        Date
    FROM gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary
    WHERE IsActive = true
    GROUP BY Name, Date
    """)

def main():
    """Main function to orchestrate the data processing"""
    spark = create_spark_session()
    
    # Read source data
    customer_df, order_df = read_source_data(spark)
    
    # Clean and transform data
    customer_clean, order_clean = clean_and_transform_data(customer_df, order_df)
    
    # Save to delta tables (steps 1-4)
    customer_clean.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customer")
    order_clean.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.order")
    
    # Create or update ordersummary table (steps 5-8)
    create_or_update_ordersummary(spark, customer_clean, order_clean)
    
    # Create and populate customeraggregatespend table (steps 9-10)
    create_customer_aggregate_spend(spark)
    
    print("Data processing completed successfully")

if __name__ == "__main__":
    main()