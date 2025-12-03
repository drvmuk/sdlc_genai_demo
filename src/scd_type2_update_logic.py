from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import TimestampType

def update_scd_type2_table(spark, catalog, schema):
    """
    Update the SCD Type 2 ordersummary table when customer data changes.
    This is a standalone function that can be called outside of DLT pipelines
    when needed.
    
    Args:
        spark: SparkSession
        catalog: The catalog name
        schema: The schema name
    """
    # Read current customer and order data
    customer_df = spark.table(f"{catalog}.{schema}.customer_silver")
    order_df = spark.table(f"{catalog}.{schema}.order_silver")
    
    # Read current ordersummary table
    ordersummary_table = DeltaTable.forName(spark, f"{catalog}.{schema}.ordersummary")
    ordersummary_df = ordersummary_table.toDF()
    
    # Generate new records based on current customer and order data
    new_records = (
        order_df
        .join(
            customer_df,
            on="CustId",
            how="inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date"
        )
        .withColumn("IsActive", F.lit(True))
        .withColumn("StartDate", F.current_timestamp())
        .withColumn("EndDate", F.lit(None).cast(TimestampType()))
    )
    
    # Find records that need to be updated (where customer details have changed)
    join_condition = (
        (ordersummary_df["CustId"] == new_records["CustId"]) &
        (ordersummary_df["OrderId"] == new_records["OrderId"]) &
        (
            (ordersummary_df["Name"] != new_records["Name"]) |
            (ordersummary_df["EmailId"] != new_records["EmailId"]) |
            (ordersummary_df["Region"] != new_records["Region"])
        ) &
        (ordersummary_df["IsActive"] == True)
    )
    
    # Perform the SCD Type 2 update
    ordersummary_table.alias("target").merge(
        new_records.alias("source"),
        join_condition
    ).whenMatchedUpdate(
        set={
            "IsActive": F.lit(False),
            "EndDate": F.current_timestamp()
        }
    ).execute()
    
    # Insert new records for the updated customers
    matched_records = (
        new_records.join(
            ordersummary_df.filter(F.col("IsActive") == False),
            on=["CustId", "OrderId"],
            how="inner"
        )
        .select(new_records["*"])
    )
    
    if matched_records.count() > 0:
        matched_records.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.ordersummary")