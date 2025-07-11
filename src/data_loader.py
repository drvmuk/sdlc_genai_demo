from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging

class DataLoader:
    """Class for loading data from CSV files into Delta tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def validate_schema(self, df: DataFrame, expected_columns: list) -> bool:
        """Validate if the DataFrame has the expected columns."""
        try:
            df_columns = df.columns
            missing_columns = [col for col in expected_columns if col not in df_columns]
            
            if missing_columns:
                self.logger.error(f"Missing columns: {missing_columns}")
                return False
            return True
        except Exception as e:
            self.logger.error(f"Error validating schema: {str(e)}")
            return False
    
    def load_customer_data(self, source_path: str, target_table: str) -> bool:
        """Load customer data from CSV to Delta table."""
        try:
            # Expected columns for customer data
            expected_columns = ["CustId", "Name", "EmailId", "Region"]
            
            # Read CSV file
            self.logger.info(f"Reading customer data from {source_path}")
            customer_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            
            # Validate schema
            if not self.validate_schema(customer_df, expected_columns):
                return False
            
            # Write to Delta table
            self.logger.info(f"Writing customer data to Delta table {target_table}")
            customer_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            
            self.logger.info("Customer data loaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error loading customer data: {str(e)}")
            return False
    
    def load_order_data(self, source_path: str, target_table: str) -> bool:
        """Load order data from CSV to Delta table."""
        try:
            # Expected columns for order data
            expected_columns = ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
            
            # Read CSV file
            self.logger.info(f"Reading order data from {source_path}")
            order_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            
            # Validate schema
            if not self.validate_schema(order_df, expected_columns):
                return False
            
            # Write to Delta table
            self.logger.info(f"Writing order data to Delta table {target_table}")
            order_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            
            self.logger.info("Order data loaded successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error loading order data: {str(e)}")
            return False