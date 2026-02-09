"""
Main module for orchestrating the transaction analytics pipeline.
"""
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame

from transaction_analytics.src.data_loader import (
    get_spark_session, load_transactions, load_customer_data, load_store_data
)
from transaction_analytics.src.transformer import (
    clean_transaction_data, enrich_transaction_data,
    calculate_customer_metrics, calculate_category_metrics,
    calculate_regional_metrics, identify_top_customers
)


def run_pipeline(
    date: str = None,
    transaction_path: str = None,
    customer_path: str = None,
    store_path: str = None,
    output_path: str = None
) -> None:
    """
    Run the transaction analytics pipeline.
    
    Args:
        date: Processing date in YYYY-MM-DD format
        transaction_path: Path to transaction data
        customer_path: Path to customer data
        store_path: Path to store data
        output_path: Base path for output data
    """
    # Initialize Spark session
    spark = get_spark_session()
    
    # Set default paths if not provided
    if not date:
        date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    if not transaction_path:
        transaction_path = f"/data/transactions/date={date}"
    if not customer_path:
        customer_path = "/data/reference/customers"
    if not store_path:
        store_path = "/data/reference/stores"
    if not output_path:
        output_path = f"/data/analytics/date={date}"
    
    print(f"Starting transaction analytics pipeline for date: {date}")
    
    # Load data
    try:
        transactions_df = load_transactions(spark, transaction_path)
        customer_df = load_customer_data(spark, customer_path)
        store_df = load_store_data(spark, store_path)
        
        print(f"Loaded {transactions_df.count()} transactions")
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise
    
    # Process data
    try:
        # Clean and enrich data
        clean_df = clean_transaction_data(transactions_df)
        enriched_df = enrich_transaction_data(clean_df, customer_df, store_df)
        
        # Calculate metrics
        customer_metrics = calculate_customer_metrics(enriched_df)
        category_metrics = calculate_category_metrics(enriched_df)
        regional_metrics = calculate_regional_metrics(enriched_df)
        top_customers = identify_top_customers(customer_metrics)
        
        # Save results
        enriched_df.write.mode("overwrite").format("delta").save(f"{output_path}/enriched_transactions")
        customer_metrics.write.mode("overwrite").format("delta").save(f"{output_path}/customer_metrics")
        category_metrics.write.mode("overwrite").format("delta").save(f"{output_path}/category_metrics")
        regional_metrics.write.mode("overwrite").format("delta").save(f"{output_path}/regional_metrics")
        top_customers.write.mode("overwrite").format("delta").save(f"{output_path}/top_customers")
        
        print("Transaction analytics pipeline completed successfully")
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run transaction analytics pipeline")
    parser.add_argument("--date", type=str, help="Processing date (YYYY-MM-DD)")
    parser.add_argument("--transaction-path", type=str, help="Path to transaction data")
    parser.add_argument("--customer-path", type=str, help="Path to customer data")
    parser.add_argument("--store-path", type=str, help="Path to store data")
    parser.add_argument("--output-path", type=str, help="Base path for output data")
    
    args = parser.parse_args()
    
    run_pipeline(
        date=args.date,
        transaction_path=args.transaction_path,
        customer_path=args.customer_path,
        store_path=args.store_path,
        output_path=args.output_path
    )