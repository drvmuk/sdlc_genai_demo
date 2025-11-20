from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .getOrCreate()

def read_customer_data(spark):
    """Read customer data from volume."""
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    return spark.read.format("csv") \
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
    
    return spark.read.format("csv") \
        .option("header", "true") \
        .schema(order_schema) \
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")

def clean_data(df):
    """Remove nulls and duplicates from dataframe."""
    return df.dropna().dropDuplicates()

def process_order_data(order_df):
    """Add TotalAmount column and clean order data."""
    return order_df \
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty")) \
        .dropDuplicates() \
        .na.drop()

def create_or_update_ordersummary(spark, customer_df, order_df):
    """
    Create or update the ordersummary table with SCD Type 2 implementation.
    """
    CATALOG = "gen_ai_poc_databrickscoe"
    SCHEMA = "sdlc_wizard"
    TABLE = "ordersummary"
    
    # Create catalog and schema if they don't exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    
    # Join customer and order data
    joined_df = customer_df.join(
        order_df,
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
        order_df["Date"]
    )
    
    # Check if the table exists
    table_exists = spark._jsparkSession.catalog().tableExists(CATALOG, SCHEMA, TABLE)
    
    if not table_exists:
        # First-time load - create the table with SCD Type 2 columns
        joined_df = joined_df \
            .withColumn("IsActive", F.lit(True)) \
            .withColumn("StartDate", F.current_timestamp()) \
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
        
        joined_df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
        
        return joined_df
    else:
        # Get existing data
        existing_df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}")
        
        # Add SCD Type 2 columns to new data
        joined_df = joined_df \
            .withColumn("IsActive", F.lit(True)) \
            .withColumn("StartDate", F.current_timestamp()) \
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
        
        # Find records that need to be updated (customer details changed)
        changed_records = joined_df.join(
            existing_df.filter(F.col("IsActive") == True),
            on=["CustId", "OrderId"],
            how="inner"
        ).filter(
            (joined_df["Name"] != existing_df["Name"]) |
            (joined_df["EmailId"] != existing_df["EmailId"]) |
            (joined_df["Region"] != existing_df["Region"])
        ).select(
            existing_df["CustId"],
            existing_df["OrderId"]
        ).distinct()
        
        # Update existing records (mark as inactive)
        records_to_update = existing_df.join(
            changed_records,
            on=["CustId", "OrderId"],
            how="inner"
        ).filter(F.col("IsActive") == True) \
         .withColumn("IsActive", F.lit(False)) \
         .withColumn("EndDate", F.current_timestamp())
        
        # New records to insert
        new_records = joined_df.join(
            existing_df.select("CustId", "OrderId").filter(F.col("IsActive") == True),
            on=["CustId", "OrderId"],
            how="left_anti"
        )
        
        # Combine all records for update
        records_to_insert = new_records.unionByName(
            joined_df.join(
                changed_records,
                on=["CustId", "OrderId"],
                how="inner"
            )
        )
        
        # Merge into target table
        spark.sql(f"""
            MERGE INTO {CATALOG}.{SCHEMA}.{TABLE} target
            USING (
                SELECT * FROM {records_to_update.createOrReplaceTempView("records_to_update")}
                records_to_update
            ) source
            ON target.CustId = source.CustId AND target.OrderId = source.OrderId AND target.IsActive = True
            WHEN MATCHED THEN
                UPDATE SET
                    target.IsActive = False,
                    target.EndDate = current_timestamp()
        """)
        
        # Insert new records
        records_to_insert.write.format("delta") \
            .mode("append") \
            .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
        
        return spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}")

def create_customer_aggregate_spend(spark, ordersummary_df):
    """
    Create the customeraggregatespend table with aggregated spending data.
    """
    CATALOG = "gen_ai_poc_databrickscoe"
    SCHEMA = "sdlc_wizard"
    TABLE = "customeraggregatespend"
    
    # Create catalog and schema if they don't exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    
    # Calculate TotalAmount and aggregate by Name and Date
    aggregate_df = ordersummary_df \
        .filter(F.col("IsActive") == True) \
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty")) \
        .groupBy("Name", "Date") \
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    
    # Write to target table
    aggregate_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
    
    return aggregate_df

def main():
    """Main function to run the batch pipeline."""
    spark = create_spark_session()
    
    # Read source data
    customer_df = read_customer_data(spark)
    order_df = read_order_data(spark)
    
    # Clean data
    customer_df = clean_data(customer_df)
    order_df = process_order_data(order_df)
    
    # Create or update ordersummary table
    ordersummary_df = create_or_update_ordersummary(spark, customer_df, order_df)
    
    # Create customer aggregate spend table
    create_customer_aggregate_spend(spark, ordersummary_df)
    
    spark.stop()

if __name__ == "__main__":
    main()