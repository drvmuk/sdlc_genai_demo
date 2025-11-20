from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, datediff, expr
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

def read_source_data(spark, customer_path, order_path):
    """
    Read source CSV data from the specified paths
    """
    # Define schemas for customer and order data
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
        .load(customer_path)
    
    # Read order data
    order_df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(order_schema) \
        .load(order_path)
    
    return customer_df, order_df

def clean_and_transform_data(customer_df, order_df):
    """
    Clean data by removing nulls and duplicates, and add TotalAmount column to order data
    """
    # Remove null values from customer data
    clean_customer_df = customer_df.filter(
        col("CustId").isNotNull() & 
        col("Name").isNotNull() & 
        col("EmailId").isNotNull() & 
        col("Region").isNotNull()
    ).dropDuplicates()
    
    # Remove null values from order data and add TotalAmount column
    clean_order_df = order_df.filter(
        col("OrderId").isNotNull() & 
        col("ItemName").isNotNull() & 
        col("PricePerUnit").isNotNull() & 
        col("Qty").isNotNull() & 
        col("Date").isNotNull() & 
        col("CustId").isNotNull()
    ).dropDuplicates() \
     .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    return clean_customer_df, clean_order_df

def create_and_update_scd_type2_table(spark, customer_df, order_df, catalog, schema):
    """
    Create and update SCD Type 2 table by joining customer and order data
    """
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
    
    # Check if the target table exists
    table_exists = False
    try:
        spark.sql(f"DESCRIBE TABLE {catalog}.{schema}.ordersummary")
        table_exists = True
    except:
        table_exists = False
    
    if not table_exists:
        # Create the table with SCD Type 2 columns
        joined_df = joined_df.withColumn("IsActive", lit(True)) \
                           .withColumn("StartDate", current_timestamp()) \
                           .withColumn("EndDate", lit(None).cast(TimestampType()))
        
        # Create the table
        joined_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{catalog}.{schema}.ordersummary")
    else:
        # Update the existing SCD Type 2 table
        delta_table = DeltaTable.forName(spark, f"{catalog}.{schema}.ordersummary")
        
        # Prepare the data for merging
        update_df = joined_df.withColumn("mergeKey", expr("CONCAT(CustId, '|', OrderId)"))
        target_df = delta_table.toDF().withColumn("mergeKey", expr("CONCAT(CustId, '|', OrderId)"))
        
        # Perform the merge operation
        delta_table.alias("target").merge(
            update_df.alias("source"),
            "target.mergeKey = source.mergeKey"
        ).whenMatchedAndExpr(
            """
            target.Name <> source.Name OR 
            target.EmailId <> source.EmailId OR 
            target.Region <> source.Region OR 
            target.ItemName <> source.ItemName OR 
            target.PricePerUnit <> source.PricePerUnit OR 
            target.Qty <> source.Qty OR 
            target.Date <> source.Date OR
            target.TotalAmount <> source.TotalAmount
            """
        ).updateExpr(
            {
                "IsActive": "false",
                "EndDate": "current_timestamp()"
            }
        ).whenNotMatchedInsertExpr(
            {
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
                "StartDate": "current_timestamp()",
                "EndDate": "null"
            }
        ).execute()
        
        # Insert new records for updated rows
        updated_records = delta_table.toDF().filter(
            (col("IsActive") == False) & (col("EndDate") == current_timestamp())
        )
        
        if updated_records.count() > 0:
            new_active_records = updated_records.select(
                "CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", 
                "PricePerUnit", "Qty", "Date", "TotalAmount"
            ).withColumn("IsActive", lit(True)) \
             .withColumn("StartDate", current_timestamp()) \
             .withColumn("EndDate", lit(None).cast(TimestampType()))
            
            new_active_records.write.format("delta") \
                .mode("append") \
                .saveAsTable(f"{catalog}.{schema}.ordersummary")

def create_customer_aggregate_spend(spark, catalog, schema):
    """
    Create customer aggregate spend table
    """
    # Check if the table exists
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customeraggregatespend (Name STRING, TotalAmount DOUBLE, Date DATE)")
    except:
        pass
    
    # Aggregate data from ordersummary table
    aggregate_df = spark.sql(f"""
        SELECT Name, SUM(TotalAmount) as TotalAmount, Date
        FROM {catalog}.{schema}.ordersummary
        WHERE IsActive = true
        GROUP BY Name, Date
    """)
    
    # Write to target table
    aggregate_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog}.{schema}.customeraggregatespend")

def main():
    spark = SparkSession.builder \
        .appName("Customer Order Processing") \
        .getOrCreate()
    
    # Paths to source data
    customer_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
    order_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
    
    # Catalog and schema
    catalog = "gen_ai_poc_databrickscoe"
    schema = "sdlc_wizard"
    
    # Read source data
    customer_df, order_df = read_source_data(spark, customer_path, order_path)
    
    # Clean and transform data
    clean_customer_df, clean_order_df = clean_and_transform_data(customer_df, order_df)
    
    # Create and update SCD Type 2 table
    create_and_update_scd_type2_table(spark, clean_customer_df, clean_order_df, catalog, schema)
    
    # Create customer aggregate spend table
    create_customer_aggregate_spend(spark, catalog, schema)

if __name__ == "__main__":
    main()