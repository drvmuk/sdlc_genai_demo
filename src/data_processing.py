from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, sum as spark_sum
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import datetime

def get_spark_session():
    """Get or create a Spark session"""
    return SparkSession.builder.appName("CustomerOrderDataProcessing").getOrCreate()

def read_source_data(spark, customer_path, order_path):
    """Read source CSV data from volume paths"""
    customer_df = spark.read.option("header", "true").option("inferSchema", "true").csv(customer_path)
    order_df = spark.read.option("header", "true").option("inferSchema", "true").csv(order_path)
    return customer_df, order_df

def clean_data(df):
    """Remove null and duplicate records from dataframe"""
    # Drop rows with any null values
    df_no_nulls = df.na.drop()
    # Drop duplicate rows
    df_clean = df_no_nulls.dropDuplicates()
    return df_clean

def process_order_data(order_df):
    """Add TotalAmount column to order dataframe"""
    return order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))

def create_ordersummary_table(spark, catalog, schema):
    """Create ordersummary table if not exists"""
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ordersummary (
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

def create_customeraggregatespend_table(spark, catalog, schema):
    """Create customeraggregatespend table if not exists"""
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    ) USING DELTA
    """)

def join_and_load_scd2(spark, customer_df, order_df, catalog, schema):
    """Join customer and order data and load to SCD type 2 table"""
    # Create the table if it doesn't exist
    create_ordersummary_table(spark, catalog, schema)
    
    # Join customer and order data
    joined_df = customer_df.join(order_df, "CustId", "inner").select(
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
    
    # Add SCD Type 2 columns
    current_time = current_timestamp()
    joined_df_with_scd = joined_df.withColumn("IsActive", lit(True)) \
                                 .withColumn("StartDate", current_time) \
                                 .withColumn("EndDate", lit(None))
    
    # Check if the target table exists and has data
    table_exists = spark._jsparkSession.catalog().tableExists(f"{catalog}.{schema}.ordersummary")
    
    if table_exists:
        # Get the existing data
        delta_table = DeltaTable.forName(spark, f"{catalog}.{schema}.ordersummary")
        target_df = delta_table.toDF()
        
        # Identify records that need to be updated (where customer data changed)
        join_condition = "source.CustId = target.CustId AND target.IsActive = true"
        
        # Find changed records
        matched_records = joined_df_with_scd.alias("source") \
            .join(target_df.alias("target"), expr(join_condition)) \
            .where("""
                source.Name != target.Name OR 
                source.EmailId != target.EmailId OR 
                source.Region != target.Region
            """)
        
        # Extract customer IDs that need to be updated
        customer_ids_to_update = matched_records.select("source.CustId").distinct()
        
        if not customer_ids_to_update.isEmpty():
            # Update existing records (set IsActive = false and EndDate = current_time)
            delta_table.update(
                condition="CustId IN (SELECT CustId FROM {}) AND IsActive = true".format(
                    customer_ids_to_update.createOrReplaceTempView("customer_ids_to_update")),
                set={
                    "IsActive": "false",
                    "EndDate": "current_timestamp()"
                }
            )
            
            # Insert new active records
            records_to_insert = joined_df_with_scd.join(
                customer_ids_to_update, 
                joined_df_with_scd.CustId == customer_ids_to_update.CustId
            )
            
            records_to_insert.write.format("delta").mode("append") \
                .saveAsTable(f"{catalog}.{schema}.ordersummary")
        
        # Insert records for new customers (not in the update list)
        new_records = joined_df_with_scd.join(
            target_df.select("CustId").distinct(),
            "CustId",
            "leftanti"
        )
        
        if not new_records.isEmpty():
            new_records.write.format("delta").mode("append") \
                .saveAsTable(f"{catalog}.{schema}.ordersummary")
    else:
        # First-time load
        joined_df_with_scd.write.format("delta").mode("overwrite") \
            .saveAsTable(f"{catalog}.{schema}.ordersummary")
    
    return joined_df_with_scd

def aggregate_customer_spend(spark, catalog, schema):
    """Aggregate TotalAmount by Name and Date and load to customeraggregatespend table"""
    # Create the table if it doesn't exist
    create_customeraggregatespend_table(spark, catalog, schema)
    
    # Read from ordersummary table
    ordersummary_df = spark.table(f"{catalog}.{schema}.ordersummary")
    
    # Aggregate data
    aggregated_df = ordersummary_df.filter(col("IsActive") == True) \
        .groupBy("Name", "Date") \
        .agg(spark_sum("TotalAmount").alias("TotalAmount"))
    
    # Write to customeraggregatespend table
    aggregated_df.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{catalog}.{schema}.customeraggregatespend")
    
    return aggregated_df

def main():
    """Main function to execute the data processing pipeline"""
    spark = get_spark_session()
    
    # Define paths and catalog/schema
    customer_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
    order_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
    catalog = "gen_ai_poc_databrickscoe"
    schema = "sdlc_wizard"
    
    # Read source data
    customer_df, order_df = read_source_data(spark, customer_path, order_path)
    
    # Clean data
    customer_df_clean = clean_data(customer_df)
    order_df_clean = clean_data(order_df)
    
    # Process order data
    order_df_processed = process_order_data(order_df_clean)
    
    # Join and load to SCD type 2 table
    join_and_load_scd2(spark, customer_df_clean, order_df_processed, catalog, schema)
    
    # Aggregate customer spend
    aggregate_customer_spend(spark, catalog, schema)
    
    print("Data processing completed successfully")

if __name__ == "__main__":
    main()