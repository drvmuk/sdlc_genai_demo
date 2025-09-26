from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, datediff, expr, sum as sum_
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from delta.tables import DeltaTable

def read_source_data(spark, customer_path, order_path):
    """
    Read customer and order data from source paths
    """
    customer_df = spark.read.option("header", "true").option("inferSchema", "true").csv(customer_path)
    order_df = spark.read.option("header", "true").option("inferSchema", "true").csv(order_path)
    
    return customer_df, order_df

def clean_data(df):
    """
    Remove null and duplicate records from dataframe
    """
    # Drop rows with any null values
    df_no_nulls = df.dropna()
    
    # Drop duplicate rows
    df_clean = df_no_nulls.dropDuplicates()
    
    return df_clean

def process_order_data(order_df):
    """
    Process order data by adding TotalAmount column
    """
    # Add TotalAmount column as PricePerUnit * Qty
    order_df_with_total = order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
    
    return order_df_with_total

def create_ordersummary_table(spark, catalog, schema):
    """
    Create ordersummary table if it doesn't exist
    """
    table_name = f"{catalog}.{schema}.ordersummary"
    
    # Check if table exists
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    if not tables.filter(col("tableName") == "ordersummary").count() > 0:
        # Create table with SCD Type 2 structure
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
    
    return table_name

def create_customeraggregatespend_table(spark, catalog, schema):
    """
    Create customeraggregatespend table if it doesn't exist
    """
    table_name = f"{catalog}.{schema}.customeraggregatespend"
    
    # Check if table exists
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    if not tables.filter(col("tableName") == "customeraggregatespend").count() > 0:
        # Create table
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Name STRING,
            TotalAmount DOUBLE,
            Date DATE
        ) USING DELTA
        """)
    
    return table_name

def update_scd_type2_table(spark, customer_df, order_df, catalog, schema):
    """
    Update the SCD Type 2 ordersummary table with changes from customer and order data
    """
    table_name = f"{catalog}.{schema}.ordersummary"
    
    # Join customer and order data
    joined_df = customer_df.join(order_df, "CustId")
    
    # Add SCD Type 2 columns to the new data
    current_time = current_timestamp()
    joined_df = joined_df.withColumn("IsActive", lit(True)) \
                         .withColumn("StartDate", current_time) \
                         .withColumn("EndDate", lit(None))
    
    # Check if the table exists and has data
    try:
        target_table = DeltaTable.forName(spark, table_name)
        
        # Identify changes by joining with existing data
        join_condition = """
            source.CustId = target.CustId AND
            source.OrderId = target.OrderId
        """
        
        # Match condition for detecting changes in customer attributes
        match_condition = """
            source.Name <> target.Name OR
            source.EmailId <> target.EmailId OR
            source.Region <> target.Region
        """
        
        # Perform SCD Type 2 merge operation
        target_table.alias("target").merge(
            joined_df.alias("source"),
            join_condition
        ).whenMatchedAnd(f"target.IsActive = true AND ({match_condition})").updateExpr(
            {
                "IsActive": "false",
                "EndDate": "current_timestamp()"
            }
        ).whenNotMatchedInsertAll().execute()
        
        # Insert new records for changed customers
        changed_records = spark.sql(f"""
            SELECT source.*
            FROM {table_name} target
            JOIN (
                SELECT * FROM {table_name} WHERE IsActive = false AND EndDate = current_timestamp()
            ) changed
            ON target.CustId = changed.CustId AND target.OrderId = changed.OrderId
            JOIN {joined_df.createOrReplaceTempView("source_data")}
            source ON source.CustId = target.CustId
        """)
        
        if changed_records.count() > 0:
            changed_records.write.format("delta").mode("append").saveAsTable(table_name)
            
    except:
        # If table doesn't exist or is empty, just insert all data
        joined_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

def aggregate_customer_spend(spark, catalog, schema):
    """
    Aggregate TotalAmount by customer and date and save to customeraggregatespend table
    """
    ordersummary_table = f"{catalog}.{schema}.ordersummary"
    customeraggregatespend_table = f"{catalog}.{schema}.customeraggregatespend"
    
    # Aggregate data
    agg_df = spark.sql(f"""
        SELECT 
            Name, 
            Date, 
            SUM(TotalAmount) as TotalAmount
        FROM {ordersummary_table}
        WHERE IsActive = true
        GROUP BY Name, Date
    """)
    
    # Write to target table
    agg_df.write.format("delta").mode("overwrite").saveAsTable(customeraggregatespend_table)

def main():
    spark = SparkSession.builder \
        .appName("Customer Order Processing") \
        .getOrCreate()
    
    # Define paths and table names
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
    
    # Create tables if they don't exist
    create_ordersummary_table(spark, catalog, schema)
    create_customeraggregatespend_table(spark, catalog, schema)
    
    # Update SCD Type 2 table
    update_scd_type2_table(spark, customer_df_clean, order_df_processed, catalog, schema)
    
    # Aggregate customer spend
    aggregate_customer_spend(spark, catalog, schema)

if __name__ == "__main__":
    main()