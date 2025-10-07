# Standard batch processing implementation (non-DLT version)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, expr, sum as sum_
from delta.tables import DeltaTable

def create_spark_session():
    """Create a Spark session for batch processing"""
    return (SparkSession.builder
            .appName("Customer Order Analytics")
            .getOrCreate())

def process_customer_order_data(spark):
    """Process customer and order data according to requirements"""
    
    # Define constants
    CATALOG = "gen_ai_poc_databrickscoe"
    SCHEMA = "sdlc_wizard"
    CUSTOMER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
    ORDER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
    
    # 1. Read source CSV data
    customer_df = (spark.read.format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .load(CUSTOMER_PATH))
    
    order_df = (spark.read.format("csv")
               .option("header", "true")
               .option("inferSchema", "true")
               .load(ORDER_PATH))
    
    # 4. Remove nulls and duplicates from customer data
    customer_df = (customer_df
                  .dropDuplicates(["CustId"])
                  .filter(
                      (col("CustId").isNotNull()) &
                      (col("Name").isNotNull()) &
                      (col("EmailId").isNotNull()) &
                      (col("Region").isNotNull())
                  ))
    
    # 3 & 4. Add TotalAmount column, remove nulls and duplicates from order data
    order_df = (order_df
               .dropDuplicates(["OrderId"])
               .filter(
                   (col("OrderId").isNotNull()) &
                   (col("ItemName").isNotNull()) &
                   (col("PricePerUnit").isNotNull()) &
                   (col("Qty").isNotNull()) &
                   (col("Date").isNotNull()) &
                   (col("CustId").isNotNull())
               )
               .withColumn("TotalAmount", col("PricePerUnit") * col("Qty")))
    
    # Save cleaned data to Delta tables
    customer_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customer")
    order_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.order")
    
    # 5 & 6. Create ordersummary table and implement SCD Type 2 logic
    # Join customer and order data
    joined_data = (customer_df
                  .join(order_df, "CustId", "inner")
                  .select(
                      "CustId", "Name", "EmailId", "Region", 
                      "OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
                  ))
    
    # Check if ordersummary table exists
    table_exists = spark._jsparkSession.catalog().tableExists(CATALOG, SCHEMA, "ordersummary")
    
    if table_exists:
        # Implement SCD Type 2 logic
        delta_table = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.ordersummary")
        
        # Get active records
        active_records = delta_table.toDF().filter(col("IsActive") == True)
        
        # Find changed records
        changed_records = (joined_data
                          .join(active_records, ["CustId", "OrderId"], "inner")
                          .filter(
                              (col("Name") != col("active_records.Name")) |
                              (col("EmailId") != col("active_records.EmailId")) |
                              (col("Region") != col("active_records.Region"))
                          )
                          .select(col("CustId"), col("OrderId")))
        
        # Update existing records - expire old ones and insert new ones
        delta_table.alias("target").merge(
            changed_records.alias("source"),
            "target.CustId = source.CustId AND target.OrderId = source.OrderId AND target.IsActive = true"
        ).whenMatched().updateExpr({
            "IsActive": "false",
            "EndDate": "current_timestamp()"
        }).execute()
        
        # Insert new active records for changed data
        new_active_records = (joined_data
                             .join(changed_records, ["CustId", "OrderId"], "inner")
                             .withColumn("IsActive", lit(True))
                             .withColumn("StartDate", current_timestamp())
                             .withColumn("EndDate", lit(None).cast("timestamp")))
        
        new_active_records.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.ordersummary")
        
        # Insert completely new records
        new_records = (joined_data
                      .join(active_records.select("CustId", "OrderId"), ["CustId", "OrderId"], "left_anti")
                      .withColumn("IsActive", lit(True))
                      .withColumn("StartDate", current_timestamp())
                      .withColumn("EndDate", lit(None).cast("timestamp")))
        
        new_records.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.ordersummary")
        
    else:
        # Create initial table with all records active
        (joined_data
         .withColumn("IsActive", lit(True))
         .withColumn("StartDate", current_timestamp())
         .withColumn("EndDate", lit(None).cast("timestamp"))
         .write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(f"{CATALOG}.{SCHEMA}.ordersummary"))
    
    # 9 & 10. Create customeraggregatespend table with aggregated data
    aggregate_df = (spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
                   .filter(col("IsActive") == True)
                   .groupBy("Name", "Date")
                   .agg(sum_("TotalAmount").alias("TotalAmount")))
    
    aggregate_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customeraggregatespend")
    
    return {
        "customer": customer_df,
        "order": order_df,
        "ordersummary": spark.table(f"{CATALOG}.{SCHEMA}.ordersummary"),
        "customeraggregatespend": aggregate_df
    }

if __name__ == "__main__":
    spark = create_spark_session()
    results = process_customer_order_data(spark)
    print("Processing completed successfully")