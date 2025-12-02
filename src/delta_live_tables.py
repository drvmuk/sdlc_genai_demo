from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import dlt

# Define source data paths
CUSTOMER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Define catalog and schema
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

@dlt.table(
    name="customer_bronze",
    comment="Raw customer data from source"
)
def customer_bronze():
    return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(CUSTOMER_PATH)

@dlt.table(
    name="order_bronze",
    comment="Raw order data from source"
)
def order_bronze():
    return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ORDER_PATH)

@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data with duplicates and nulls removed"
)
def customer_silver():
    # Read from bronze layer
    df = dlt.read("customer_bronze")
    
    # Remove nulls and duplicates
    return df.dropDuplicates(["CustId"]) \
             .filter(F.col("CustId").isNotNull() & 
                     F.col("Name").isNotNull() & 
                     F.col("EmailId").isNotNull() & 
                     F.col("Region").isNotNull())

@dlt.table(
    name="order_silver",
    comment="Cleaned order data with TotalAmount calculated"
)
def order_silver():
    # Read from bronze layer
    df = dlt.read("order_bronze")
    
    # Remove nulls and duplicates, and add TotalAmount column
    return df.dropDuplicates(["OrderId"]) \
             .filter(F.col("OrderId").isNotNull() & 
                     F.col("ItemName").isNotNull() & 
                     F.col("PricePerUnit").isNotNull() & 
                     F.col("Qty").isNotNull() & 
                     F.col("Date").isNotNull() & 
                     F.col("CustId").isNotNull()) \
             .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))

@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table combining customer and order data",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
def ordersummary():
    # Read the latest customer and order data
    customer_df = dlt.read("customer_silver")
    order_df = dlt.read("order_silver")
    
    # Join customer and order data
    joined_df = order_df.join(customer_df, "CustId", "inner")
    
    # Select columns as per the required schema
    current_data = joined_df.select(
        "CustId", "Name", "EmailId", "Region", 
        "OrderId", "ItemName", "PricePerUnit", "Qty", "Date"
    )
    
    # Check if the target table exists
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    
    try:
        # Try to read the existing table
        target_table = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.ordersummary")
        
        # Identify changes (SCD Type 2 implementation)
        # Use a hash of non-key columns to detect changes
        current_data = current_data.withColumn(
            "row_hash", 
            F.sha2(
                F.concat_ws("||", 
                           F.col("Name"), 
                           F.col("EmailId"), 
                           F.col("Region"),
                           F.col("ItemName"),
                           F.col("PricePerUnit"),
                           F.col("Qty"),
                           F.col("Date")), 
                256
            )
        )
        
        # Add current timestamp for new records
        current_data = current_data.withColumn("StartDate", F.current_timestamp())
        current_data = current_data.withColumn("EndDate", F.lit(None).cast("timestamp"))
        current_data = current_data.withColumn("IsActive", F.lit(True))
        
        # Convert target to DataFrame for comparison
        target_df = target_table.toDF()
        
        # Add row_hash to target for comparison
        target_with_hash = target_df.withColumn(
            "row_hash", 
            F.sha2(
                F.concat_ws("||", 
                           F.col("Name"), 
                           F.col("EmailId"), 
                           F.col("Region"),
                           F.col("ItemName"),
                           F.col("PricePerUnit"),
                           F.col("Qty"),
                           F.col("Date")), 
                256
            )
        )
        
        # Find records that changed
        join_condition = (current_data["CustId"] == target_with_hash["CustId"]) & \
                         (current_data["OrderId"] == target_with_hash["OrderId"])
                         
        # Records that exist in source but have different hash in target
        changed_records = current_data.join(
            target_with_hash, 
            join_condition & (current_data["row_hash"] != target_with_hash["row_hash"]) & target_with_hash["IsActive"],
            "inner"
        ).select(current_data["*"])
        
        # Records that exist in source but not in target
        new_records = current_data.join(
            target_with_hash,
            join_condition,
            "left_anti"
        )
        
        # Combine new and changed records
        updates = changed_records.union(new_records).drop("row_hash")
        
        # Update the target table - expire old records and insert new ones
        target_table.alias("target").merge(
            updates.alias("updates"),
            "target.CustId = updates.CustId AND target.OrderId = updates.OrderId AND target.IsActive = true"
        ).whenMatched().updateExpr({
            "IsActive": "false",
            "EndDate": "current_timestamp()"
        }).whenNotMatched().insertAll().execute()
        
        # Return the updated table
        return spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
        
    except:
        # If table doesn't exist, create it with the initial data
        result_df = current_data.drop("row_hash") \
            .withColumn("StartDate", F.current_timestamp()) \
            .withColumn("EndDate", F.lit(None).cast("timestamp")) \
            .withColumn("IsActive", F.lit(True))
            
        return result_df

@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date"
)
def customeraggregatespend():
    # Read from ordersummary
    ordersummary_df = dlt.read("ordersummary")
    
    # Join with order_silver to get TotalAmount
    order_df = dlt.read("order_silver")
    
    joined_df = ordersummary_df.join(
        order_df,
        ["OrderId"],
        "inner"
    )
    
    # Aggregate TotalAmount by Name and Date
    aggregated_df = joined_df.groupBy("Name", "Date") \
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    
    return aggregated_df