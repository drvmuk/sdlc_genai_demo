"""
Order Summary Processor - SCD Type 2 Implementation
This module processes customer and order data and loads it into a Slowly Changing Dimension (SCD) Type 2 table.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, lag, row_number, 
    to_date, date_format, current_date
)
from pyspark.sql.window import Window
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OrderSummaryProcessor:
    """Process customer and order data and load into SCD Type 2 table."""
    
    def __init__(self, spark=None):
        """Initialize with SparkSession."""
        self.spark = spark or SparkSession.builder.appName("OrderSummaryProcessor").getOrCreate()
        logger.info("OrderSummaryProcessor initialized")
    
    def read_csv_data(self, path, dataset_name):
        """Read CSV data from the specified path."""
        try:
            logger.info(f"Reading {dataset_name} from {path}")
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)
            logger.info(f"Successfully read {dataset_name} with schema: {df.schema}")
            return df
        except Exception as e:
            logger.error(f"Error reading {dataset_name} from {path}: {str(e)}")
            raise
    
    def clean_data(self, df, dataset_name):
        """Remove null and duplicate records from dataframe."""
        try:
            logger.info(f"Cleaning {dataset_name} data")
            # Count records before cleaning
            initial_count = df.count()
            
            # Filter out rows where any column is null or "Null"
            for column in df.columns:
                df = df.filter((col(column).isNotNull()) & (col(column) != "Null"))
            
            # Drop duplicates
            df = df.dropDuplicates()
            
            # Count records after cleaning
            final_count = df.count()
            logger.info(f"Cleaned {dataset_name}: Removed {initial_count - final_count} records")
            
            return df
        except Exception as e:
            logger.error(f"Error cleaning {dataset_name} data: {str(e)}")
            raise
    
    def join_data(self, customer_df, order_df):
        """Join customer and order data on CustId."""
        try:
            logger.info("Joining customer and order data")
            joined_df = customer_df.join(order_df, "CustId", "inner")
            logger.info(f"Joined data has {joined_df.count()} records")
            return joined_df
        except Exception as e:
            logger.error(f"Error joining customer and order data: {str(e)}")
            raise
    
    def load_to_delta_table(self, df, table_name, mode="overwrite"):
        """Load dataframe to Delta table."""
        try:
            logger.info(f"Loading data to Delta table {table_name} with mode {mode}")
            df.write.format("delta").mode(mode).saveAsTable(table_name)
            logger.info(f"Successfully loaded data to {table_name}")
        except Exception as e:
            logger.error(f"Error loading data to {table_name}: {str(e)}")
            raise
    
    def update_scd_type2(self, source_df, target_table):
        """
        Update the SCD Type 2 table with new records.
        
        This implementation:
        1. Adds StartDate, EndDate, and IsActive columns to source data
        2. Compares with existing data in the target table
        3. Updates existing records (sets EndDate and IsActive=False)
        4. Inserts new records with current date as StartDate and IsActive=True
        """
        try:
            logger.info(f"Updating SCD Type 2 table {target_table}")
            
            # Check if target table exists
            tables = self.spark.catalog.listTables()
            table_exists = any(table.name == target_table.split('.')[-1] for table in tables)
            
            # If target table doesn't exist, create it
            if not table_exists:
                logger.info(f"Target table {target_table} does not exist. Creating new table.")
                
                # Add SCD Type 2 columns to source data
                current_date_val = current_date()
                source_df = source_df.withColumn("StartDate", current_date_val) \
                                    .withColumn("EndDate", lit(None).cast("date")) \
                                    .withColumn("IsActive", lit(True))
                
                # Create the target table
                self.load_to_delta_table(source_df, target_table, "overwrite")
                return
            
            # If target table exists, read existing data
            target_df = self.spark.table(target_table)
            
            # Prepare source data with SCD Type 2 columns
            source_with_scd = source_df.withColumn("StartDate", current_date()) \
                                      .withColumn("EndDate", lit(None).cast("date")) \
                                      .withColumn("IsActive", lit(True))
            
            # Identify business keys - assume all columns except SCD tracking columns are business keys
            business_keys = [col for col in target_df.columns 
                            if col not in ["StartDate", "EndDate", "IsActive"]]
            
            # Find records that have changed
            join_condition = " AND ".join([f"target.{key} = source.{key}" for key in business_keys])
            
            # Find existing active records that need to be expired
            records_to_expire = self.spark.sql(f"""
                SELECT target.*
                FROM {target_table} target
                JOIN temp_source source
                ON {join_condition}
                WHERE target.IsActive = true
                AND (
                    {" OR ".join([f"target.{col} <> source.{col}" for col in source_df.columns if col != "CustId"])}
                )
            """)
            
            # Expire old records
            if records_to_expire.count() > 0:
                expired_records = records_to_expire \
                    .withColumn("EndDate", current_date()) \
                    .withColumn("IsActive", lit(False))
                
                # Update the target table with expired records
                expired_records.write.format("delta").mode("overwrite").option("replaceWhere", "IsActive = true").saveAsTable(target_table)
            
            # Insert new records
            source_with_scd.write.format("delta").mode("append").saveAsTable(target_table)
            
            logger.info(f"Successfully updated SCD Type 2 table {target_table}")
        except Exception as e:
            logger.error(f"Error updating SCD Type 2 table {target_table}: {str(e)}")
            raise
    
    def process(self, customer_path, order_path, target_table):
        """Main process to read, clean, join, and load data into SCD Type 2 table."""
        try:
            # Read data
            customer_df = self.read_csv_data(customer_path, "customer data")
            order_df = self.read_csv_data(order_path, "order data")
            
            # Clean data
            customer_df_clean = self.clean_data(customer_df, "customer")
            order_df_clean = self.clean_data(order_df, "order")
            
            # Load cleaned data to delta tables
            self.load_to_delta_table(customer_df_clean, "gen_ai_poc_databrickscoe.sdlc_wizard.customer", "overwrite")
            self.load_to_delta_table(order_df_clean, "gen_ai_poc_databrickscoe.sdlc_wizard.order", "overwrite")
            
            # Join data
            joined_df = self.join_data(customer_df_clean, order_df_clean)
            
            # Update SCD Type 2 table
            self.update_scd_type2(joined_df, target_table)
            
            logger.info("Process completed successfully")
            return True
        except Exception as e:
            logger.error(f"Error in process: {str(e)}")
            # Implement retry mechanism for transient errors
            # This is a simplified version - in production, you might want to use a more sophisticated retry mechanism
            logger.info("Attempting to retry process...")
            try:
                # Retry once
                # Read data
                customer_df = self.read_csv_data(customer_path, "customer data")
                order_df = self.read_csv_data(order_path, "order data")
                
                # Clean data
                customer_df_clean = self.clean_data(customer_df, "customer")
                order_df_clean = self.clean_data(order_df, "order")
                
                # Load cleaned data to delta tables
                self.load_to_delta_table(customer_df_clean, "gen_ai_poc_databrickscoe.sdlc_wizard.customer", "overwrite")
                self.load_to_delta_table(order_df_clean, "gen_ai_poc_databrickscoe.sdlc_wizard.order", "overwrite")
                
                # Join data
                joined_df = self.join_data(customer_df_clean, order_df_clean)
                
                # Update SCD Type 2 table
                self.update_scd_type2(joined_df, target_table)
                
                logger.info("Retry completed successfully")
                return True
            except Exception as retry_error:
                logger.error(f"Retry failed: {str(retry_error)}")
                return False


def main():
    """Main function to run the processor."""
    # Source and target paths
    customer_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
    order_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
    target_table = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"
    
    # Create processor and run
    processor = OrderSummaryProcessor()
    result = processor.process(customer_path, order_path, target_table)
    
    if result:
        logger.info("Order summary processing completed successfully")
    else:
        logger.error("Order summary processing failed")


if __name__ == "__main__":
    main()