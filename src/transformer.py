"""
Module for transforming transaction data.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, when, sum as spark_sum, avg, count, datediff, current_timestamp,
    date_format, year, month, dayofmonth, dayofweek, lit, rank, dense_rank
)
from typing import List, Dict, Any


def clean_transaction_data(df: DataFrame) -> DataFrame:
    """
    Clean and standardize transaction data.
    
    Args:
        df: Raw transaction DataFrame
        
    Returns:
        DataFrame: Cleaned transaction data
    """
    # Convert is_online to boolean
    cleaned_df = df.withColumn(
        "is_online", 
        when(col("is_online").isin(["true", "True", "TRUE", "1", "y", "Y", "yes", "Yes"]), True)
        .otherwise(False)
    )
    
    # Handle missing values
    cleaned_df = cleaned_df.na.fill({
        "category": "Uncategorized",
        "payment_method": "Unknown"
    })
    
    # Filter out potentially problematic records
    cleaned_df = cleaned_df.filter(col("amount").isNotNull())
    
    # Standardize category names
    cleaned_df = cleaned_df.withColumn(
        "category",
        when(col("category") == "Groceries", "Grocery")
        .when(col("category") == "Food & Dining", "Food")
        .otherwise(col("category"))
    )
    
    return cleaned_df


def enrich_transaction_data(
    transactions_df: DataFrame,
    customer_df: DataFrame,
    store_df: DataFrame
) -> DataFrame:
    """
    Enrich transaction data with customer and store information.
    
    Args:
        transactions_df: Cleaned transaction DataFrame
        customer_df: Customer reference data
        store_df: Store reference data
        
    Returns:
        DataFrame: Enriched transaction data
    """
    # Join with customer data
    enriched_df = transactions_df.join(
        customer_df.select("customer_id", "customer_segment", "loyalty_tier", "age_group"),
        on="customer_id",
        how="left"
    )
    
    # Join with store data
    enriched_df = enriched_df.join(
        store_df.select("store_id", "store_type", "region"),
        on="store_id",
        how="left"
    )
    
    # Add date dimensions
    enriched_df = enriched_df.withColumn("transaction_year", year(col("transaction_date"))) \
        .withColumn("transaction_month", month(col("transaction_date"))) \
        .withColumn("transaction_day", dayofmonth(col("transaction_date"))) \
        .withColumn("transaction_dow", dayofweek(col("transaction_date")))
    
    return enriched_df


def calculate_customer_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate customer-level metrics from transaction data.
    
    Args:
        df: Enriched transaction DataFrame
        
    Returns:
        DataFrame: Customer metrics
    """
    # Group by customer and calculate metrics
    customer_metrics = df.groupBy("customer_id") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            spark_sum("amount").alias("total_spend"),
            avg("amount").alias("avg_transaction_value"),
            spark_sum(when(col("is_online"), col("amount")).otherwise(0)).alias("online_spend"),
            spark_sum(when(~col("is_online"), col("amount")).otherwise(0)).alias("instore_spend")
        )
    
    # Calculate additional derived metrics
    customer_metrics = customer_metrics.withColumn(
        "online_spend_ratio", 
        col("online_spend") / col("total_spend")
    )
    
    return customer_metrics


def calculate_category_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate category-level metrics from transaction data.
    
    Args:
        df: Enriched transaction DataFrame
        
    Returns:
        DataFrame: Category metrics
    """
    # Group by category and calculate metrics
    category_metrics = df.groupBy("category") \
        .agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_transaction_amount")
        ) \
        .orderBy(col("total_amount").desc())
    
    return category_metrics


def calculate_regional_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate region-level metrics from transaction data.
    
    Args:
        df: Enriched transaction DataFrame
        
    Returns:
        DataFrame: Regional metrics
    """
    # Group by region and calculate metrics
    regional_metrics = df.filter(col("region").isNotNull()) \
        .groupBy("region") \
        .agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_transaction_amount"),
            count(col("customer_id")).alias("customer_count")
        ) \
        .orderBy(col("total_amount").desc())
    
    return regional_metrics


def identify_top_customers(df: DataFrame, n: int = 100) -> DataFrame:
    """
    Identify top N customers by total spend.
    
    Args:
        df: Customer metrics DataFrame
        n: Number of top customers to identify
        
    Returns:
        DataFrame: Top N customers
    """
    window_spec = Window.orderBy(col("total_spend").desc())
    
    top_customers = df.withColumn("rank", dense_rank().over(window_spec)) \
        .filter(col("rank") <= n) \
        .drop("rank")
    
    return top_customers