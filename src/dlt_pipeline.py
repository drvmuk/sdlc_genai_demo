import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Define source data paths
CUSTOMER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Define catalog and schema
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

# Step 1 & 2: Read source data and define schema
@dlt.table(
    name="customer",
    comment="Raw customer data with cleaning applied"
)
def customer():
    # Read customer data
    df = spark.read.option("inferSchema", "true").csv(CUSTOMER_DATA_PATH, header=True)
    
    # Define schema explicitly
    df = df.select(
        F.col("CustId").cast("string"),
        F.col("Name").cast("string"),
        F.col("EmailId").cast("string"),
        F.col("Region").cast("string")
    )
    
    # Step 4: Remove nulls and duplicates
    df = df.na.drop().dropDuplicates()
    
    return df

@dlt.table(
    name="order",
    comment="Raw order data with cleaning applied"
)
def order():
    # Read order data
    df = spark.read.option("inferSchema", "true").csv(ORDER_DATA_PATH, header=True)
    
    # Define schema explicitly
    df = df.select(
        F.col("OrderId").cast("string"),
        F.col("ItemName").cast("string"),
        F.col("PricePerUnit").cast("double"),
        F.col("Qty").cast("int"),
        F.col("Date").cast("date"),
        F.col("CustId").cast("string")
    )
    
    # Step 3: Add TotalAmount column
    df = df.withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    
    # Step 4: Remove nulls and duplicates
    df = df.na.drop().dropDuplicates()
    
    return df

# Step 6-8: Create SCD Type 2 table for ordersummary
@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table combining customer and order data",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_all_or_drop({"valid_custid": "CustId IS NOT NULL"})
def ordersummary():
    # Get the current data in the ordersummary table if it exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    
    # Get reference to current tables
    customer_df = dlt.read("customer")
    order_df = dlt.read("order")
    
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
    
    # Add SCD Type 2 columns
    current_timestamp = F.current_timestamp()
    joined_df = joined_df.withColumn("IsActive", F.lit(True))
    joined_df = joined_df.withColumn("StartDate", current_timestamp)
    joined_df = joined_df.withColumn("EndDate", F.lit(None).cast("timestamp"))
    
    # Check if the target table exists
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").filter(F.col("tableName") == "ordersummary").collect()
    
    if len(tables) == 0:
        # First run - just return the joined data with SCD columns
        return joined_df
    else:
        # Get existing data
        existing_df = dlt.read("ordersummary")
        
        # Identify changed records based on customer data
        # We'll use a window function to get the latest record for each customer
        window_spec = Window.partitionBy("CustId").orderBy(F.desc("StartDate"))
        
        latest_existing = existing_df.withColumn(
            "row_num", F.row_number().over(window_spec)
        ).filter(F.col("row_num") == 1).drop("row_num")
        
        # Find records that have changed
        changed_customers = customer_df.join(
            latest_existing.filter(F.col("IsActive") == True),
            on="CustId",
            how="inner"
        ).filter(
            (customer_df["Name"] != latest_existing["Name"]) | 
            (customer_df["EmailId"] != latest_existing["EmailId"]) | 
            (customer_df["Region"] != latest_existing["Region"])
        ).select(customer_df["CustId"]).distinct()
        
        # Mark existing records as inactive
        records_to_update = existing_df.join(
            changed_customers,
            on="CustId",
            how="inner"
        ).filter(F.col("IsActive") == True)
        
        if records_to_update.count() > 0:
            # Create new records for the changed customers
            new_records = joined_df.join(
                changed_customers,
                on="CustId",
                how="inner"
            )
            
            # Update existing records (mark as inactive)
            updated_existing = existing_df.join(
                changed_customers,
                on="CustId",
                how="left_anti"
            ).unionAll(
                records_to_update.withColumn("IsActive", F.lit(False))
                .withColumn("EndDate", current_timestamp)
            )
            
            # Combine updated existing records with new records
            result_df = updated_existing.unionAll(new_records)
            return result_df
        else:
            # No changes, return the existing data
            return existing_df

# Step 9-10: Create customer aggregate spend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    # Read from the ordersummary table
    ordersummary_df = dlt.read("ordersummary")
    
    # Aggregate TotalAmount by Name and Date
    aggregated_df = ordersummary_df.groupBy("Name", "Date").agg(
        F.sum("TotalAmount").alias("TotalAmount")
    )
    
    return aggregated_df