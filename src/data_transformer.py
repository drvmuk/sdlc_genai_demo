from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr
import logging

class DataTransformer:
    """Class for transforming data in Delta tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def add_total_amount(self, table_name: str) -> bool:
        """Add TotalAmount column to order table by multiplying PricePerUnit and Qty."""
        try:
            self.logger.info(f"Reading order data from Delta table {table_name}")
            order_df = self.spark.read.format("delta").table(table_name)
            
            # Check if required columns exist
            if "PricePerUnit" not in order_df.columns or "Qty" not in order_df.columns:
                self.logger.error("Required columns PricePerUnit or Qty not found")
                return False
            
            # Add TotalAmount column
            self.logger.info("Adding TotalAmount column")
            order_df_with_total = order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
            
            # Write back to Delta table
            self.logger.info(f"Writing transformed data back to Delta table {table_name}")
            order_df_with_total.write.format("delta").mode("overwrite").saveAsTable(table_name)
            
            self.logger.info("TotalAmount column added successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error adding TotalAmount column: {str(e)}")
            return False
    
    def join_customer_order_data(self, customer_table: str, order_table: str, target_table: str) -> bool:
        """Join customer and order data and save to ordersummary table."""
        try:
            self.logger.info(f"Reading customer data from Delta table {customer_table}")
            customer_df = self.spark.read.format("delta").table(customer_table)
            
            self.logger.info(f"Reading order data from Delta table {order_table}")
            order_df = self.spark.read.format("delta").table(order_table)
            
            # Join data on CustId
            self.logger.info("Joining customer and order data")
            joined_df = order_df.join(customer_df, "CustId", "inner")
            
            # Write to ordersummary Delta table
            self.logger.info(f"Writing joined data to Delta table {target_table}")
            joined_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            
            self.logger.info("Customer and order data joined successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error joining customer and order data: {str(e)}")
            return False
    
    def aggregate_customer_spend(self, source_table: str, target_table: str) -> bool:
        """Aggregate customer spend data and save to customeraggregatespend table."""
        try:
            self.logger.info(f"Reading order summary data from Delta table {source_table}")
            order_summary_df = self.spark.read.format("delta").table(source_table)
            
            # Check if required columns exist
            required_columns = ["Name", "Date", "TotalAmount"]
            for col_name in required_columns:
                if col_name not in order_summary_df.columns:
                    self.logger.error(f"Required column {col_name} not found")
                    return False
            
            # Aggregate data
            self.logger.info("Aggregating customer spend data")
            aggregated_df = order_summary_df.groupBy("Name", "Date").sum("TotalAmount").withColumnRenamed("sum(TotalAmount)", "TotalSpend")
            
            # Write to customeraggregatespend Delta table
            self.logger.info(f"Writing aggregated data to Delta table {target_table}")
            aggregated_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            
            self.logger.info("Customer spend data aggregated successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error aggregating customer spend data: {str(e)}")
            return False