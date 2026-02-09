"""Main ETL pipeline module."""

from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from .aggregate import (create_customer_analytics, create_product_analytics,
                        create_sales_summary, create_time_series_analytics)
from .config import DEFAULT_CONFIG, PipelineConfig
from .extract import extract_data
from .transform import (add_sales_metrics, clean_and_transform_data,
                        enrich_sales_data)


def get_spark_session(app_name: str = "ETL Pipeline") -> SparkSession:
    """
    Get or create a SparkSession.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession instance
    """
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def save_dataframe(
    df: DataFrame,
    path: str,
    format: str = "delta",
    mode: str = "overwrite",
    partition_by: Optional[list] = None
) -> None:
    """
    Save a DataFrame to storage.
    
    Args:
        df: DataFrame to save
        path: Target path
        format: Output format (delta, parquet, etc.)
        mode: Write mode (overwrite, append, etc.)
        partition_by: List of columns to partition by
    """
    writer = df.write.format(format).mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.save(path)


def run_pipeline(
    source_path: Optional[str] = None,
    target_path: Optional[str] = None,
    date: Optional[str] = None,
    config: PipelineConfig = None
) -> Dict[str, DataFrame]:
    """
    Run the complete ETL pipeline.
    
    Args:
        source_path: Optional override for source path
        target_path: Optional override for target path
        date: Optional date filter
        config: Pipeline configuration
        
    Returns:
        Dictionary of output DataFrames
    """
    # Use default config if none provided
    if config is None:
        config = DEFAULT_CONFIG
    
    # Override paths if provided
    if source_path:
        for source in config.sources:
            source.path = source_path
    
    if target_path:
        config.target_path = target_path
    
    # Initialize Spark session
    spark = get_spark_session()
    
    # Extract data
    dataframes = extract_data(spark, config.sources, date)
    
    # Transform data
    sales_df = clean_and_transform_data(dataframes["sales"])
    enriched_df = enrich_sales_data(
        sales_df,
        dataframes["products"],
        dataframes["customers"]
    )
    metrics_df = add_sales_metrics(enriched_df)
    
    # Aggregate data
    sales_summary = create_sales_summary(metrics_df)
    product_analytics = create_product_analytics(metrics_df)
    customer_analytics = create_customer_analytics(metrics_df)
    time_series = create_time_series_analytics(metrics_df)
    
    # Save results
    results = {
        "sales_enriched": metrics_df,
        "sales_summary": sales_summary,
        "product_analytics": product_analytics,
        "customer_analytics": customer_analytics,
        "time_series": time_series
    }
    
    for name, df in results.items():
        output_path = f"{config.target_path}/{name}"
        save_dataframe(
            df,
            output_path,
            format=config.target_format,
            mode=config.write_mode,
            partition_by=config.partition_by
        )
    
    return results


if __name__ == "__main__":
    # Example of running the pipeline
    run_pipeline(
        source_path="dbfs:/mnt/data/raw/",
        target_path="dbfs:/mnt/data/processed/",
        date="2023-10-01"
    )