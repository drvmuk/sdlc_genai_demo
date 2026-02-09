"""Data transformation module for the ETL pipeline."""

from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_and_transform_data(sales_df: DataFrame) -> DataFrame:
    """
    Clean and transform sales data.
    
    Args:
        sales_df: Raw sales DataFrame
        
    Returns:
        Cleaned and transformed DataFrame
    """
    # Handle missing values
    cleaned_df = sales_df.na.fill(0, ["quantity", "unit_price"])
    
    # Drop rows with missing critical fields
    cleaned_df = cleaned_df.dropna(subset=["sale_id", "product_id", "date"])
    
    # Add calculated columns
    transformed_df = cleaned_df.withColumn(
        "total_amount", F.col("quantity") * F.col("unit_price")
    ).withColumn(
        "year", F.year(F.to_date(F.col("date")))
    ).withColumn(
        "month", F.month(F.to_date(F.col("date")))
    ).withColumn(
        "day", F.dayofmonth(F.to_date(F.col("date")))
    )
    
    return transformed_df


def enrich_sales_data(
    sales_df: DataFrame, 
    products_df: DataFrame,
    customers_df: DataFrame
) -> DataFrame:
    """
    Enrich sales data with product and customer information.
    
    Args:
        sales_df: Sales DataFrame
        products_df: Products DataFrame
        customers_df: Customers DataFrame
        
    Returns:
        Enriched sales DataFrame
    """
    # Join with products
    enriched_df = sales_df.join(
        products_df.select("product_id", "product_name", "category"),
        on="product_id",
        how="left"
    )
    
    # Join with customers
    enriched_df = enriched_df.join(
        customers_df.select("customer_id", "customer_name", "region"),
        on="customer_id",
        how="left"
    )
    
    return enriched_df


def add_sales_metrics(sales_df: DataFrame) -> DataFrame:
    """
    Add advanced sales metrics to the DataFrame.
    
    Args:
        sales_df: Sales DataFrame
        
    Returns:
        DataFrame with additional metrics
    """
    # Define window specifications
    window_by_product = Window.partitionBy("product_id")
    window_by_customer = Window.partitionBy("customer_id")
    window_by_date_region = Window.partitionBy("date", "region")
    
    # Add metrics
    metrics_df = sales_df.withColumn(
        "product_total_sales", F.sum("total_amount").over(window_by_product)
    ).withColumn(
        "customer_total_sales", F.sum("total_amount").over(window_by_customer)
    ).withColumn(
        "daily_region_rank", F.rank().over(
            window_by_date_region.orderBy(F.desc("total_amount"))
        )
    )
    
    return metrics_df