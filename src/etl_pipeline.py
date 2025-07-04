"""
Main module for running the ETL pipeline.
"""
from src.data_ingestion import ingest_data
from src.data_cleansing import cleanse_data
from src.table_management import create_ordersummary_table
from src.data_transformation import join_customer_order_data
from src.scd_management import load_scd_type2, automate_scd_updates
from src.logging_utils import log_info, log_error, log_start_job, log_end_job

def run_etl_pipeline():
    """
    Run the full ETL pipeline.
    """
    log_start_job("Full ETL Pipeline")
    
    try:
        # Step 1: Ingest raw customer and order data
        customer_df, order_df = ingest_data()
        
        # Step 2: Cleanse customer and order data
        cleansed_customer_df, cleansed_order_df = cleanse_data(customer_df, order_df)
        
        # Step 3: Create ordersummary table if it doesn't exist
        create_ordersummary_table()
        
        # Step 4: Join customer and order data
        joined_df = join_customer_order_data(cleansed_customer_df, cleansed_order_df)
        
        # Step 5: Load joined data into ordersummary table using SCD Type 2 logic
        load_scd_type2(joined_df)
        
        log_info("ETL pipeline completed successfully")
        log_end_job("Full ETL Pipeline")
    
    except Exception as e:
        log_error("ETL pipeline failed", e)
        raise

def run_automated_updates():
    """
    Run automated updates to ordersummary table based on changes in customer data.
    """
    log_start_job("Automated Updates")
    
    try:
        # Ingest updated customer data and current order data
        updated_customer_df, order_df = ingest_data()
        
        # Cleanse data
        cleansed_customer_df, cleansed_order_df = cleanse_data(updated_customer_df, order_df)
        
        # Automate updates to ordersummary table
        automate_scd_updates(cleansed_customer_df, cleansed_order_df)
        
        log_info("Automated updates completed successfully")
        log_end_job("Automated Updates")
    
    except Exception as e:
        log_error("Automated updates failed", e)
        raise

if __name__ == "__main__":
    run_etl_pipeline()