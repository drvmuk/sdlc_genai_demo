from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import dlt

# Define source data paths
CUSTOMER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
ORDER_DATA_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"

# Define catalog and schema
CATALOG = "gen_ai_poc_databrickscoe"
SCHEMA = "sdlc_wizard"

@dlt.table(
    name="customer",
    comment="Customer data cleaned from source"
)
def customer():
    """
    Load and clean customer data from source
    """
    # Read customer data
    customer_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(CUSTOMER_DATA_PATH)
    
    # Clean data: Remove nulls and duplicates
    cleaned_df = (customer_df
                 .dropDuplicates(["CustId"])
                 .filter(F.col("CustId").isNotNull() & 
                         F.col("Name").isNotNull() & 
                         F.col("EmailId").isNotNull() & 
                         F.col("Region").isNotNull()))
    
    return cleaned_df

@dlt.table(
    name="order",
    comment="Order data cleaned from source with TotalAmount calculated"
)
def order():
    """
    Load and clean order data from source, add TotalAmount column
    """
    # Read order data
    order_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ORDER_DATA_PATH)
    
    # Add TotalAmount column and clean data
    cleaned_df = (order_df
                 .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
                 .dropDuplicates(["OrderId"])
                 .filter(F.col("OrderId").isNotNull() & 
                         F.col("ItemName").isNotNull() & 
                         F.col("PricePerUnit").isNotNull() & 
                         F.col("Qty").isNotNull() & 
                         F.col("Date").isNotNull() & 
                         F.col("CustId").isNotNull()))
    
    return cleaned_df

@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table joining customer and order data"
)
def ordersummary():
    """
    Create SCD Type 2 table by joining customer and order data
    """
    # Get customer and order data
    customer_df = dlt.read("customer")
    order_df = dlt.read("order")
    
    # Check if ordersummary table exists
    try:
        # Try to read the existing table to check if it exists
        existing_table = spark.table(f"{CATALOG}.{SCHEMA}.ordersummary")
        table_exists = True
    except:
        table_exists = False
    
    # Join customer and order data
    joined_df = (customer_df
                .join(order_df, "CustId", "inner")
                .select(
                    "CustId",
                    "Name",
                    "EmailId",
                    "Region",
                    "OrderId",
                    "ItemName",
                    "PricePerUnit",
                    "Qty",
                    "Date",
                    "TotalAmount"
                ))
    
    if not table_exists:
        # First time load - add SCD Type 2 columns
        result_df = (joined_df
                    .withColumn("IsActive", F.lit(True))
                    .withColumn("StartDate", F.current_timestamp())
                    .withColumn("EndDate", F.lit(None).cast("timestamp")))
        return result_df
    else:
        # Get existing data
        existing_df = existing_table.filter(F.col("IsActive") == True)
        
        # Find records with changes in customer data
        changed_customers = (customer_df
                           .join(
                               existing_df.select("CustId", "Name", "EmailId", "Region").distinct(),
                               "CustId",
                               "inner"
                           )
                           .filter(
                               (F.col("Name") != existing_df["Name"]) |
                               (F.col("EmailId") != existing_df["EmailId"]) |
                               (F.col("Region") != existing_df["Region"])
                           )
                           .select("CustId")
                           .distinct())
        
        # Update existing records (make them inactive)
        records_to_update = (existing_df
                           .join(changed_customers, "CustId", "inner")
                           .withColumn("IsActive", F.lit(False))
                           .withColumn("EndDate", F.current_timestamp()))
        
        # Create new records for changed customers
        new_records = (joined_df
                      .join(changed_customers, "CustId", "inner")
                      .withColumn("IsActive", F.lit(True))
                      .withColumn("StartDate", F.current_timestamp())
                      .withColumn("EndDate", F.lit(None).cast("timestamp")))
        
        # Combine existing unchanged records, updated records, and new records
        unchanged_records = existing_df.join(changed_customers, "CustId", "leftanti")
        
        result_df = unchanged_records.union(records_to_update).union(new_records)
        return result_df

@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending data"
)
def customeraggregatespend():
    """
    Aggregate TotalAmount by Name and Date
    """
    # Get ordersummary data
    ordersummary_df = dlt.read("ordersummary")
    
    # Aggregate data
    aggregated_df = (ordersummary_df
                    .filter(F.col("IsActive") == True)
                    .groupBy("Name", "Date")
                    .agg(F.sum("TotalAmount").alias("TotalAmount")))
    
    return aggregated_df