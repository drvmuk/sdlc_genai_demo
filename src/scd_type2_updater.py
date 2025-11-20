from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

def update_scd_type2(spark, catalog, schema):
    """
    This function handles the SCD Type 2 updates for the ordersummary table
    when there are changes in the customer table.
    """
    # Read the current customer and ordersummary tables
    customer_df = spark.table(f"{catalog}.{schema}.customer")
    ordersummary_df = spark.table(f"{catalog}.{schema}.ordersummary").filter("IsActive = true")
    
    # Join to identify records with customer changes
    changed_records = ordersummary_df.alias("o").join(
        customer_df.alias("c"),
        col("o.CustId") == col("c.CustId"),
        "inner"
    ).filter(
        (col("o.Name") != col("c.Name")) | 
        (col("o.EmailId") != col("c.EmailId")) | 
        (col("o.Region") != col("c.Region"))
    ).select("o.*")
    
    # If there are changed records, update the SCD Type 2 table
    if changed_records.count() > 0:
        # Get the Delta table
        delta_table = DeltaTable.forName(spark, f"{catalog}.{schema}.ordersummary")
        
        # Update existing records to mark them as inactive
        delta_table.alias("target").merge(
            changed_records.alias("source"),
            "target.CustId = source.CustId AND target.IsActive = true"
        ).whenMatched().updateExpr({
            "IsActive": "false",
            "EndDate": "current_timestamp()"
        }).execute()
        
        # Create new records with updated customer information
        new_records = changed_records.join(
            customer_df,
            "CustId",
            "inner"
        ).select(
            customer_df["CustId"],
            customer_df["Name"],
            customer_df["EmailId"],
            customer_df["Region"],
            changed_records["OrderId"],
            changed_records["ItemName"],
            changed_records["PricePerUnit"],
            changed_records["Qty"],
            changed_records["Date"],
            changed_records["TotalAmount"],
            lit(True).alias("IsActive"),
            current_timestamp().alias("StartDate"),
            lit(None).cast(TimestampType()).alias("EndDate")
        )
        
        # Insert the new records
        new_records.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.ordersummary")
        
        # Update the customeraggregatespend table
        update_customer_aggregate_spend(spark, catalog, schema)

def update_customer_aggregate_spend(spark, catalog, schema):
    """Update the customeraggregatespend table after SCD Type 2 updates"""
    # Read active records from ordersummary
    ordersummary_df = spark.table(f"{catalog}.{schema}.ordersummary").filter("IsActive = true")
    
    # Aggregate by Name and Date
    aggregated_df = ordersummary_df.groupBy("Name", "Date") \
                                  .agg({"TotalAmount": "sum"}) \
                                  .withColumnRenamed("sum(TotalAmount)", "TotalAmount")
    
    # Write to customeraggregatespend table
    aggregated_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.customeraggregatespend")

def main():
    spark = SparkSession.builder \
        .appName("SCD Type 2 Updater") \
        .getOrCreate()
    
    # Configuration
    catalog = "gen_ai_poc_databrickscoe"
    schema = "sdlc_wizard"
    
    # Update SCD Type 2 table
    update_scd_type2(spark, catalog, schema)

if __name__ == "__main__":
    main()