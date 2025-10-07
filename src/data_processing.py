"""
Core data processing functions for the customer order analytics pipeline.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

def clean_customer_data(df: DataFrame) -> DataFrame:
    """
    Clean customer data by removing nulls and duplicates.
    
    Args:
        df: Raw customer DataFrame
        
    Returns:
        Cleaned customer DataFrame
    """
    return (
        df.filter(
            col("CustId").isNotNull() &
            col("Name").isNotNull() &
            col("EmailId").isNotNull() &
            col("Region").isNotNull()
        )
        .dropDuplicates(["CustId"])
    )

def process_order_data(df: DataFrame) -> DataFrame:
    """
    Process order data by calculating total amount and removing nulls/duplicates.
    
    Args:
        df: Raw order DataFrame
        
    Returns:
        Processed order DataFrame with TotalAmount column
    """
    return (
        df.filter(
            col("OrderId").isNotNull() &
            col("ItemName").isNotNull() &
            col("PricePerUnit").isNotNull() &
            col("Qty").isNotNull() &
            col("Date").isNotNull() &
            col("CustId").isNotNull()
        )
        .withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        .dropDuplicates(["OrderId"])
    )

def join_customer_order_data(customer_df: DataFrame, order_df: DataFrame) -> DataFrame:
    """
    Join customer and order data.
    
    Args:
        customer_df: Cleaned customer DataFrame
        order_df: Processed order DataFrame
        
    Returns:
        Joined DataFrame with customer and order information
    """
    return (
        customer_df.join(
            order_df,
            on="CustId",
            how="inner"
        )
        .select(
            "CustId", "Name", "EmailId", "Region", "OrderId", 
            "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
        )
    )

def aggregate_customer_spend(df: DataFrame) -> DataFrame:
    """
    Aggregate customer spending by name and date.
    
    Args:
        df: DataFrame with customer order data
        
    Returns:
        Aggregated customer spending DataFrame
    """
    return (
        df.filter(col("IsActive") == True)  # Only use active records
        .groupBy("Name", "Date")
        .agg({"TotalAmount": "sum"})
        .withColumnRenamed("sum(TotalAmount)", "TotalAmount")
        .select("Name", "TotalAmount", "Date")
    )