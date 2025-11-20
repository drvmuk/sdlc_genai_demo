from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, datediff, expr, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from delta.tables import DeltaTable
import datetime

def read_customer_data(spark, source_path):
    """
    Read customer data from source path
    """
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    return (spark.read
            .option("header", "true")
            .schema(customer_schema)
            .csv(source_path)
            .dropDuplicates()
            .filter(col("CustId").isNotNull() & 
                   col("Name").isNotNull() & 
                   col("EmailId").isNotNull() & 
                   col("Region").isNotNull()))

def read_order_data(spark, source_path):
    """
    Read order data from source path
    """
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    return (spark.read
            .option("header", "true")
            .schema(order_schema)
            .csv(source_path)
            .dropDuplicates()
            .filter(col("OrderId").isNotNull() & 
                   col("ItemName").isNotNull() & 
                   col("PricePerUnit").isNotNull() & 
                   col("Qty").isNotNull() & 
                   col("Date").isNotNull() & 
                   col("CustId").isNotNull())
            .withColumn("TotalAmount", col("PricePerUnit") * col("Qty")))

def create_or_update_scd2_table(spark, customer_df, order_df, catalog, schema):
    """
    Create or update SCD Type 2 table by joining customer and order data
    """
    # Check if the table exists
    table_exists = False
    try:
        spark.sql(f"SELECT 1 FROM {catalog}.{schema}.ordersummary LIMIT 1")
        table_exists = True
    except:
        pass
    
    # Join customer and order data
    joined_df = (customer_df.join(order_df, "CustId")
                 .select("CustId", "Name", "EmailId", "Region", 
                         "OrderId", "ItemName", "PricePerUnit", "Qty", "Date"))
    
    if not table_exists:
        # First time load - create the table with SCD2 fields
        (joined_df
         .withColumn("IsActive", lit(True))
         .withColumn("StartDate", current_timestamp())
         .withColumn("EndDate", lit(None).cast(TimestampType()))
         .write
         .format("delta")
         .mode("overwrite")
         .saveAsTable(f"{catalog}.{schema}.ordersummary"))
        return
    
    # For updates, implement SCD Type 2 logic
    target_table = DeltaTable.forName(spark, f"{catalog}.{schema}.ordersummary")
    
    # Identify changes in customer data
    join_condition = "target.CustId = source.CustId"
    update_condition = """
        target.Name <> source.Name OR
        target.EmailId <> source.EmailId OR
        target.Region <> source.Region
    """
    
    # Perform the merge operation
    (target_table.alias("target")
     .merge(
         joined_df.alias("source"),
         join_condition)
     .whenMatchedAnd(update_condition)
     .updateExpr({
         "IsActive": "false",
         "EndDate": "current_timestamp()"
     })
     .whenNotMatchedInsertExpr({
         "CustId": "source.CustId",
         "Name": "source.Name",
         "EmailId": "source.EmailId",
         "Region": "source.Region",
         "OrderId": "source.OrderId",
         "ItemName": "source.ItemName",
         "PricePerUnit": "source.PricePerUnit",
         "Qty": "source.Qty",
         "Date": "source.Date",
         "IsActive": "true",
         "StartDate": "current_timestamp()",
         "EndDate": "null"
     })
     .execute())
    
    # Insert new records for updated customers
    updated_customers = (target_table.toDF()
                         .filter(col("EndDate").isNotNull() & 
                                (col("EndDate") > current_timestamp() - expr("INTERVAL 1 HOUR")))
                         .select("CustId", "Name", "EmailId", "Region"))
    
    if updated_customers.count() > 0:
        new_records = (updated_customers.join(order_df, "CustId")
                       .select("CustId", "Name", "EmailId", "Region", 
                               "OrderId", "ItemName", "PricePerUnit", "Qty", "Date")
                       .withColumn("IsActive", lit(True))
                       .withColumn("StartDate", current_timestamp())
                       .withColumn("EndDate", lit(None).cast(TimestampType())))
        
        new_records.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.ordersummary")

def create_customer_aggregate_spend(spark, catalog, schema):
    """
    Create customer aggregate spend table
    """
    # Create the table if it doesn't exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customeraggregatespend (
            Name STRING,
            TotalAmount DOUBLE,
            Date DATE
        )
        USING DELTA
    """)
    
    # Aggregate data from ordersummary
    agg_df = (spark.table(f"{catalog}.{schema}.ordersummary")
              .filter(col("IsActive") == True)
              .groupBy("Name", "Date")
              .agg(spark_sum("PricePerUnit" * "Qty").alias("TotalAmount")))
    
    # Write to target table
    agg_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.customeraggregatespend")

def run_batch_processing(spark):
    """
    Run the batch processing pipeline
    """
    # Configuration
    catalog = "gen_ai_poc_databrickscoe"
    schema = "sdlc_wizard"
    customer_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
    order_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
    
    # Read source data
    customer_df = read_customer_data(spark, customer_path)
    order_df = read_order_data(spark, order_path)
    
    # Create or update SCD2 table
    create_or_update_scd2_table(spark, customer_df, order_df, catalog, schema)
    
    # Create customer aggregate spend
    create_customer_aggregate_spend(spark, catalog, schema)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Customer Order Processing").getOrCreate()
    run_batch_processing(spark)