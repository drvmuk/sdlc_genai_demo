from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, datediff, expr, sum as sum_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
from delta.tables import DeltaTable

def read_customer_data(spark, path):
    """Read customer data from the specified path"""
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    return spark.read.option("header", "true").schema(customer_schema).csv(path)

def read_order_data(spark, path):
    """Read order data from the specified path"""
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    return spark.read.option("header", "true").schema(order_schema).csv(path)

def clean_data(df):
    """Remove null and duplicate records from dataframe"""
    # Remove rows with any null values
    df_no_nulls = df.dropna()
    
    # Remove duplicate rows
    df_clean = df_no_nulls.dropDuplicates()
    
    return df_clean

def calculate_total_amount(order_df):
    """Add TotalAmount column to order dataframe"""
    return order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))

def join_customer_order(customer_df, order_df):
    """Join customer and order data using CustId"""
    return customer_df.join(order_df, "CustId", "inner").select(
        "CustId", "Name", "EmailId", "Region", "OrderId", "ItemName", 
        "PricePerUnit", "Qty", "Date", "TotalAmount"
    )

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
    )
    USING DELTA
    """)

def update_scd_type2_table(spark, catalog, schema, joined_df):
    """Update SCD Type 2 table with new data"""
    # Add SCD Type 2 columns to the new data
    new_data = joined_df.withColumn("IsActive", lit(True)) \
                        .withColumn("StartDate", current_timestamp()) \
                        .withColumn("EndDate", lit(None).cast(TimestampType()))
    
    # Check if the table exists and has data
    table_exists = spark._jsparkSession.catalog().tableExists(f"{catalog}.{schema}.ordersummary")
    
    if table_exists:
        # Get the existing table as a DeltaTable
        delta_table = DeltaTable.forName(spark, f"{catalog}.{schema}.ordersummary")
        
        # Identify the matching records between new data and existing data
        condition = """
            target.CustId = source.CustId AND
            target.OrderId = source.OrderId AND
            target.IsActive = true AND
            (
                target.Name != source.Name OR
                target.EmailId != source.EmailId OR
                target.Region != source.Region OR
                target.ItemName != source.ItemName OR
                target.PricePerUnit != source.PricePerUnit OR
                target.Qty != source.Qty OR
                target.Date != source.Date OR
                target.TotalAmount != source.TotalAmount
            )
        """
        
        # Perform the merge operation
        delta_table.alias("target") \
            .merge(
                new_data.alias("source"),
                condition
            ) \
            .whenMatched() \
            .updateExpr({
                "IsActive": "false",
                "EndDate": "current_timestamp()"
            }) \
            .whenNotMatched() \
            .insertAll() \
            .execute()
        
        # Insert new records for the updated ones
        updated_records = delta_table.toDF().filter("IsActive = false AND EndDate = current_timestamp()")
        if updated_records.count() > 0:
            updated_data = updated_records \
                .join(new_data, ["CustId", "OrderId"], "inner") \
                .select(
                    new_data["CustId"],
                    new_data["Name"],
                    new_data["EmailId"],
                    new_data["Region"],
                    new_data["OrderId"],
                    new_data["ItemName"],
                    new_data["PricePerUnit"],
                    new_data["Qty"],
                    new_data["Date"],
                    new_data["TotalAmount"],
                    lit(True).alias("IsActive"),
                    current_timestamp().alias("StartDate"),
                    lit(None).cast(TimestampType()).alias("EndDate")
                )
            
            updated_data.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.ordersummary")
    else:
        # If table doesn't exist or is empty, just insert the new data
        new_data.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.ordersummary")

def create_customer_aggregate_spend_table(spark, catalog, schema):
    """Create customeraggregatespend table if not exists"""
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customeraggregatespend (
        Name STRING,
        TotalAmount DOUBLE,
        Date DATE
    )
    USING DELTA
    """)

def aggregate_customer_spend(spark, catalog, schema):
    """Aggregate TotalAmount by Name and Date from ordersummary"""
    # Read from ordersummary table
    ordersummary_df = spark.table(f"{catalog}.{schema}.ordersummary").filter("IsActive = true")
    
    # Aggregate by Name and Date
    aggregated_df = ordersummary_df.groupBy("Name", "Date") \
                                  .agg(sum_("TotalAmount").alias("TotalAmount"))
    
    # Write to customeraggregatespend table
    aggregated_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.customeraggregatespend")

def main():
    spark = SparkSession.builder \
        .appName("Customer Order Processing") \
        .getOrCreate()
    
    # Configuration
    catalog = "gen_ai_poc_databrickscoe"
    schema = "sdlc_wizard"
    customer_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
    order_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
    
    # 1. Read source data
    customer_df = read_customer_data(spark, customer_path)
    order_df = read_order_data(spark, order_path)
    
    # 2 & 4. Clean data (remove nulls and duplicates)
    customer_df_clean = clean_data(customer_df)
    order_df_clean = clean_data(order_df)
    
    # 3. Add TotalAmount column
    order_df_with_total = calculate_total_amount(order_df_clean)
    
    # Write cleaned data to delta tables
    customer_df_clean.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.customer")
    order_df_with_total.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.order")
    
    # 5. Create ordersummary table if not exists
    create_ordersummary_table(spark, catalog, schema)
    
    # 6 & 7. Join customer and order data
    joined_df = join_customer_order(customer_df_clean, order_df_with_total)
    
    # 8. Update SCD Type 2 table
    update_scd_type2_table(spark, catalog, schema, joined_df)
    
    # 9. Create customeraggregatespend table if not exists
    create_customer_aggregate_spend_table(spark, catalog, schema)
    
    # 10. Aggregate and load data
    aggregate_customer_spend(spark, catalog, schema)

if __name__ == "__main__":
    main()