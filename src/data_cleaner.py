from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging

class DataCleaner:
    """Class for cleaning data in Delta tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def remove_nulls_and_duplicates(self, table_name: str, primary_key_cols: list) -> bool:
        """Remove null values and duplicate records from a Delta table."""
        try:
            self.logger.info(f"Reading data from Delta table {table_name}")
            df = self.spark.read.format("delta").table(table_name)
            
            # Get initial count
            initial_count = df.count()
            self.logger.info(f"Initial record count: {initial_count}")
            
            # Remove null values
            self.logger.info("Removing null values")
            columns = df.columns
            for column in columns:
                df = df.filter(col(column).isNotNull())
            
            # Count after removing nulls
            after_nulls_count = df.count()
            self.logger.info(f"Record count after removing nulls: {after_nulls_count}")
            self.logger.info(f"Removed {initial_count - after_nulls_count} records with null values")
            
            # Remove duplicates
            self.logger.info(f"Removing duplicate records based on keys: {primary_key_cols}")
            df = df.dropDuplicates(primary_key_cols)
            
            # Count after removing duplicates
            final_count = df.count()
            self.logger.info(f"Final record count: {final_count}")
            self.logger.info(f"Removed {after_nulls_count - final_count} duplicate records")
            
            # Write back to Delta table
            self.logger.info(f"Writing cleaned data back to Delta table {table_name}")
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            
            self.logger.info("Data cleaning completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error cleaning data: {str(e)}")
            return False
    
    def clean_customer_data(self, table_name: str) -> bool:
        """Clean customer data by removing nulls and duplicates."""
        try:
            # For customer data, CustId is the primary key
            return self.remove_nulls_and_duplicates(table_name, ["CustId"])
        except Exception as e:
            self.logger.error(f"Error cleaning customer data: {str(e)}")
            return False
    
    def clean_order_data(self, table_name: str) -> bool:
        """Clean order data by removing nulls and duplicates."""
        try:
            # For order data, OrderId is the primary key
            return self.remove_nulls_and_duplicates(table_name, ["OrderId"])
        except Exception as e:
            self.logger.error(f"Error cleaning order data: {str(e)}")
            return False