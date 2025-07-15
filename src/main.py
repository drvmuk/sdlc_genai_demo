"""
Main entry point for the Delta data processing pipeline
"""
import logging
from src.load_data import load_csv_to_delta
from src.cleanse_data import cleanse_delta_tables
from src.order_summary import create_order_summary_table, load_order_summary_table, update_order_summary_on_customer_change
from src.customer_aggregate import create_customer_aggregate_spend_table, load_customer_aggregate_spend_table

def run_pipeline():
    """
    Run the complete Delta data processing pipeline
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        logging.info("Starting Delta data processing pipeline")
        
        # Step 1: Load CSV data into Delta tables (TR-DELTA-001)
        logging.info("Step 1: Loading CSV data into Delta tables")
        load_csv_to_delta()
        
        # Step 2: Cleanse Delta tables (TR-DELTA-002)
        logging.info("Step 2: Cleansing Delta tables")
        cleanse_delta_tables()
        
        # Step 3: Create order summary table (TR-DELTA-003)
        logging.info("Step 3: Creating order summary table")
        create_order_summary_table()
        
        # Step 4: Load order summary table (TR-DELTA-004)
        logging.info("Step 4: Loading order summary table")
        load_order_summary_table()
        
        # Step 5: Update order summary on customer change (TR-DELTA-005)
        logging.info("Step 5: Updating order summary on customer change")
        update_order_summary_on_customer_change()
        
        # Step 6: Create customer aggregate spend table (TR-DELTA-006)
        logging.info("Step 6: Creating customer aggregate spend table")
        create_customer_aggregate_spend_table()
        
        # Step 7: Load customer aggregate spend table (TR-DELTA-007)
        logging.info("Step 7: Loading customer aggregate spend table")
        load_customer_aggregate_spend_table()
        
        logging.info("Delta data processing pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Error running Delta data processing pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    run_pipeline()