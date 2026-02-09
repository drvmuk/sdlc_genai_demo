"""Data aggregation module for the ETL pipeline."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def create_sales_summary(sales_df: DataFrame) -> DataFrame:
    """
    Create a summary of sales data.
    
    Args:
        sales_df: Enriched sales DataFrame
        
    Returns:
        Summary DataFrame with aggregated metrics
    """
    summary_df = sales_df.groupBy("date", "region", "category").agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_amount").alias("total_sales"),
        F.count("sale_id").alias("transaction_count"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.avg("unit_price").alias("average_price")
    )
    
    return summary_df


def create_product_analytics(sales_df: DataFrame) -> DataFrame:
    """
    Create product analytics from sales data.
    
    Args:
        sales_df: Enriched sales DataFrame
        
    Returns:
        Product analytics DataFrame
    """
    product_analytics = sales_df.groupBy("product_id", "product_name", "category").agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("unit_price").alias("average_price"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("date").alias("days_sold")
    ).withColumn(
        "revenue_per_day", F.col("total_revenue") / F.col("days_sold")
    )
    
    return product_analytics


def create_customer_analytics(sales_df: DataFrame) -> DataFrame:
    """
    Create customer analytics from sales data.
    
    Args:
        sales_df: Enriched sales DataFrame
        
    Returns:
        Customer analytics DataFrame
    """
    customer_analytics = sales_df.groupBy("customer_id", "customer_name", "region").agg(
        F.sum("total_amount").alias("total_spend"),
        F.count("sale_id").alias("transaction_count"),
        F.countDistinct("date").alias("shopping_days"),
        F.countDistinct("product_id").alias("unique_products"),
        F.max("date").alias("last_purchase_date")
    ).withColumn(
        "average_transaction_value", F.col("total_spend") / F.col("transaction_count")
    )
    
    return customer_analytics


def create_time_series_analytics(sales_df: DataFrame) -> DataFrame:
    """
    Create time series analytics from sales data.
    
    Args:
        sales_df: Enriched sales DataFrame
        
    Returns:
        Time series analytics DataFrame
    """
    time_series = sales_df.groupBy("date", "region").agg(
        F.sum("total_amount").alias("daily_sales"),
        F.count("sale_id").alias("transaction_count"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.avg("total_amount").alias("average_basket_size")
    )
    
    return time_series