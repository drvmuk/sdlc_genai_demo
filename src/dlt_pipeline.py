"""
Delta Live Tables Pipeline for Customer and Order Data Processing.

This module implements a data processing pipeline using Delta Live Tables
to process customer and order data, implement SCD Type 2 logic, and
aggregate spend data by customer and date.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Configuration
SOURCE_CUSTOMER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
SOURCE_ORDER_PATH = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
TARGET_DATABASE = "gen_ai_poc_databrickscoe.sdlc_wizard"


@dlt.table(
    name="customer",
    comment="Cleaned and transformed customer data",
    table_properties={"quality": "bronze"}
)
def customer_table():
    """
    Load customer data from CSV files, clean, and transform it.
    
    Returns:
        DataFrame: Cleaned and transformed customer data.
    """
    # Read customer data from CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(SOURCE_CUSTOMER_PATH)
    
    # Clean data: remove nulls in mandatory columns
    df = df.filter(
        F.col("CustId").isNotNull() &
        F.col("Name").isNotNull() &
        F.col("EmailId").isNotNull() &
        F.col("Region").isNotNull()
    )
    
    # Remove duplicates based on CustId
    df = df.dropDuplicates(["CustId"])
    
    return df


@dlt.table(
    name="order",
    comment="Cleaned and transformed order data with TotalAmount column",
    table_properties={"quality": "bronze"}
)
def order_table():
    """
    Load order data from CSV files, clean, transform, and add TotalAmount column.
    
    Returns:
        DataFrame: Cleaned and transformed order data with TotalAmount column.
    """
    # Read order data from CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(SOURCE_ORDER_PATH)
    
    # Clean data: remove nulls in mandatory columns
    df = df.filter(
        F.col("OrderId").isNotNull() &
        F.col("ItemName").isNotNull() &
        F.col("PricePerUnit").isNotNull() &
        F.col("Qty").isNotNull() &
        F.col("Date").isNotNull() &
        F.col("CustId").isNotNull()
    )
    
    # Remove duplicates based on OrderId
    df = df.dropDuplicates(["OrderId"])
    
    # Add TotalAmount column
    df = df.withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    
    return df


@dlt.table(
    name="ordersummary",
    comment="Joined customer and order data with SCD Type 2 logic applied",
    table_properties={"quality": "silver"}
)
def ordersummary_table():
    """
    Join customer and order data, implement SCD Type 2 logic.
    
    Returns:
        DataFrame: Joined data with SCD Type 2 logic applied.
    """
    # Read from bronze tables
    customer_df = dlt.read("customer")
    order_df = dlt.read("order")
    
    # Join customer and order data
    joined_df = order_df.join(customer_df, "CustId", "inner")
    
    # Add SCD Type 2 columns
    current_date = datetime.now().strftime("%Y-%m-%d")
    joined_df = joined_df.withColumn("StartDate", F.lit(current_date))
    joined_df = joined_df.withColumn("EndDate", F.lit("9999-12-31"))
    joined_df = joined_df.withColumn("IsActive", F.lit(True))
    
    # Check if the target table exists
    try:
        # Try to read existing data
        existing_df = spark.table(f"{TARGET_DATABASE}.ordersummary")
        
        # Identify records that need updates (same OrderId but different attributes)
        join_condition = (existing_df["OrderId"] == joined_df["OrderId"])
        
        # Find changed records
        changed_records = existing_df.join(
            joined_df, 
            join_condition, 
            "inner"
        ).filter(
            (existing_df["Name"] != joined_df["Name"]) |
            (existing_df["EmailId"] != joined_df["EmailId"]) |
            (existing_df["Region"] != joined_df["Region"]) |
            (existing_df["ItemName"] != joined_df["ItemName"]) |
            (existing_df["PricePerUnit"] != joined_df["PricePerUnit"]) |
            (existing_df["Qty"] != joined_df["Qty"]) |
            (existing_df["TotalAmount"] != joined_df["TotalAmount"])
        ).select(existing_df["OrderId"])
        
        # Update existing records to be inactive
        update_df = existing_df.join(
            changed_records, 
            "OrderId", 
            "inner"
        ).withColumn("EndDate", F.lit(current_date))\
         .withColumn("IsActive", F.lit(False))
        
        # Get new records (not in existing data)
        new_records = joined_df.join(
            existing_df, 
            "OrderId", 
            "left_anti"
        )
        
        # Get changed records with new values
        changed_new = joined_df.join(
            changed_records, 
            "OrderId", 
            "inner"
        )
        
        # Combine updated existing records, new records, and unchanged records
        unchanged_records = existing_df.join(
            changed_records, 
            "OrderId", 
            "left_anti"
        ).filter(F.col("IsActive") == True)
        
        result_df = update_df.union(new_records).union(changed_new).union(unchanged_records)
        return result_df
    except:
        # If table doesn't exist, return the joined data with SCD Type 2 columns
        return joined_df


@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated spend data by customer and date",
    table_properties={"quality": "gold"}
)
def customeraggregatespend_table():
    """
    Aggregate spend data by customer and date from the ordersummary table.
    
    Returns:
        DataFrame: Aggregated spend data by customer and date.
    """
    # Read from silver table
    ordersummary_df = dlt.read("ordersummary")
    
    # Filter for active records only
    active_records = ordersummary_df.filter(F.col("IsActive") == True)
    
    # Group by Name and Date, sum TotalAmount
    aggregated_df = active_records.groupBy("Name", "Date").agg(
        F.sum("TotalAmount").alias("TotalSpend")
    )
    
    return aggregated_df