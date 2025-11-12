"""
Batch processing module for customer and order data.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, datediff, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
from delta.tables import DeltaTable


def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("Customer Order Processing") \
        .getOrCreate()


def read_customer_data(spark, source_path):
    """Read customer data from source path"""
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    return spark.read \
        .option("header", "true") \
        .schema(customer_schema) \
        .csv(source_path)


def read_order_data(spark, source_path):
    """Read order data from source path"""
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    return spark.read \
        .option("header", "true") \
        .schema(order_schema) \
        .csv(source_path)


def clean_data(df):
    """Clean data by removing nulls and duplicates"""
    # Remove rows with any null values
    df_no_nulls = df.dropna()
    
    # Remove duplicate rows
    df_clean = df_no_nulls.dropDuplicates()
    
    return df_clean


def process_order_data(order_df):
    """Process order data by adding TotalAmount column"""
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
        )
        USING DELTA
    """)


def create_customeraggregatespend_table(spark, catalog, schema):
    """Create customeraggregatespend table if not exists"""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customeraggregatespend (
            Name STRING,
            TotalAmount DOUBLE,
            Date DATE
        )
        USING DELTA
    """)


def update_scd_type2_table(spark, catalog, schema, customer_df, order_df):
    """
    Update the SCD Type 2 table with changes in customer data
    """
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
        order_df["Date"],
        order_df["TotalAmount"]
    )
    
    # Add SCD Type 2 columns to the new data
    current_time = current_timestamp()
    new_data = joined_df.withColumn("IsActive", lit(True)) \
                        .withColumn("StartDate", current_time) \
                        .withColumn("EndDate", lit(None).cast(TimestampType()))
    
    # Check if table exists
    table_exists = spark._jsparkSession.catalog().tableExists(f"{catalog}.{schema}.ordersummary")
    
    if table_exists:
        # Get the Delta table
        delta_table = DeltaTable.forName(spark, f"{catalog}.{schema}.ordersummary")
        
        # Identify the records that need to be updated (where customer details have changed)
        matched_condition = """
            target.CustId = source.CustId AND
            target.OrderId = source.OrderId AND
            target.IsActive = true AND
            (
                target.Name != source.Name OR
                target.EmailId != source.EmailId OR
                target.Region != source.Region
            )
        """
        
        # Perform the SCD Type 2 merge operation
        delta_table.alias("target").merge(
            new_data.alias("source"),
            matched_condition
        ).whenMatchedUpdate(
            condition="target.IsActive = true",
            set={
                "IsActive": "false",
                "EndDate": current_time
            }
        ).whenNotMatchedInsertAll() \
        .execute()
        
        # Insert new records for the updated customers
        updated_customers = delta_table.toDF().filter(
            (col("EndDate") == current_time) & 
            (col("IsActive") == False)
        ).select("CustId", "OrderId")
        
        if updated_customers.count() > 0:
            # Get the new records for these customers
            new_records = new_data.join(
                updated_customers,
                on=["CustId", "OrderId"],
                how="inner"
            )
            
            # Write the new active records
            new_records.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.ordersummary")
    else:
        # If table doesn't exist, just write the data
        new_data.write.format("delta").saveAsTable(f"{catalog}.{schema}.ordersummary")


def aggregate_customer_spend(spark, catalog, schema):
    """
    Aggregate TotalAmount by Name and Date and save to customeraggregatespend table
    """
    # Read from the ordersummary table
    ordersummary_df = spark.table(f"{catalog}.{schema}.ordersummary")
    
    # Aggregate the data
    aggregated_df = ordersummary_df.filter(col("IsActive") == True) \
                                   .groupBy("Name", "Date") \
                                   .sum("TotalAmount") \
                                   .withColumnRenamed("sum(TotalAmount)", "TotalAmount")
    
    # Write to the customeraggregatespend table
    aggregated_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.customeraggregatespend")


def main():
    """Main function to process customer and order data"""
    spark = create_spark_session()
    
    # Define paths and catalog/schema
    customer_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
    order_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
    catalog = "gen_ai_poc_databrickscoe"
    schema = "sdlc_wizard"
    
    # Read data
    customer_df = read_customer_data(spark, customer_path)
    order_df = read_order_data(spark, order_path)
    
    # Clean data
    customer_df_clean = clean_data(customer_df)
    order_df_clean = clean_data(order_df)
    
    # Process order data
    order_df_processed = process_order_data(order_df_clean)
    
    # Create tables if not exist
    create_ordersummary_table(spark, catalog, schema)
    create_customeraggregatespend_table(spark, catalog, schema)
    
    # Update SCD Type 2 table
    update_scd_type2_table(spark, catalog, schema, customer_df_clean, order_df_processed)
    
    # Aggregate customer spend
    aggregate_customer_spend(spark, catalog, schema)


if __name__ == "__main__":
    main()