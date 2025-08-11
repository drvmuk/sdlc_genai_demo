"""
Main module for running the data loading and processing jobs.
"""
import argparse
import logging
from data_loader import DataLoader, get_spark_session

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Main function to run the data loading and processing jobs.
    """
    parser = argparse.ArgumentParser(description='Data Loading and Processing')
    parser.add_argument('--job', type=str, required=True, 
                        choices=['load_data', 'update_summary'],
                        help='Job to run: load_data or update_summary')
    
    args = parser.parse_args()
    
    try:
        # Get SparkSession
        spark = get_spark_session()
        
        # Create DataLoader
        data_loader = DataLoader(spark)
        
        if args.job == 'load_data':
            # Load customer data
            data_loader.load_csv_to_delta(
                data_loader.customer_source_path,
                data_loader.customer_target
            )
            
            # Load order data
            data_loader.load_csv_to_delta(
                data_loader.order_source_path,
                data_loader.order_target
            )
            
            # Generate order summary
            data_loader.generate_order_summary()
            
            logger.info("Data loading and order summary generation completed successfully")
            
        elif args.job == 'update_summary':
            # Update order summary for customer changes
            data_loader.update_order_summary_for_customer_changes()
            
            logger.info("Order summary update completed successfully")
    
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()