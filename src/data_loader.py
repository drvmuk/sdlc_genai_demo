"""
Module for loading customer and order data into Delta tables and generating order summaries.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, when
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataLoader:
    """
    Class for loading customer and order data into Delta tables and generating order summaries.
    """
    def __init__(self, spark):
        """
        Initialize the DataLoader with a SparkSession.
        
        Args:
            spark: SparkSession object
        """
        self.spark = spark
        
        # Define source and target paths
        self.customer_source_path = "Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
        self.order_source_path = "Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
        
        self.customer_target = "gen_ai_poc_databrickscoe.sdlc_wizard.customer"
        self.order_target = "gen_ai_poc_databrickscoe.sdlc_wizard.order"
        self.order_summary_target = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"
    
    def load_csv_to_delta(self, source_path, target_table):
        """
        Load CSV data into a Delta table.
        
        Args:
            source_path: Path to the CSV data
            target_table: Name of the target Delta table
            
        Returns:
            DataFrame containing the loaded data
        """
        try:
            logger.info(f"Loading data from {source_path} to {target_table}")
            
            # Read CSV data
            df = self.spark.read.option("inferSchema", "true").option("header", "true").csv(source_path)
            
            # Clean data - remove nulls and duplicates
            initial_count = df.count()
            df = df.dropna()
            df = df.dropDuplicates()
            final_count = df.count()
            
            logger.info(f"Data cleaning: removed {initial_count - final_count} records (nulls and duplicates)")
            
            # Add metadata columns for SCD Type 2
            df = df.withColumn("effective_date", current_timestamp()) \
                   .withColumn("is_current", lit(True)) \
                   .withColumn("end_date", lit(None))
            
            # Write to Delta table
            df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            
            logger.info(f"Successfully loaded data into {target_table}")
            return df
        
        except Exception as e:
            logger.error(f"Error loading data from {source_path} to {target_table}: {str(e)}")
            raise
    
    def generate_order_summary(self):
        """
        Generate order summary by joining customer and order data.
        
        Returns:
            DataFrame containing the order summary
        """
        try:
            logger.info("Generating order summary")
            
            # Read customer and order data
            customer_df = self.spark.table(self.customer_target)
            order_df = self.spark.table(self.order_target)
            
            # Join customer and order data
            order_summary_df = order_df.join(
                customer_df,
                order_df.CustId == customer_df.CustId,
                "inner"
            ).select(
                order_df["*"],
                customer_df.col("Name").alias("CustomerName"),
                customer_df.col("Address"),
                customer_df.col("Phone"),
                customer_df.col("Email"),
                current_timestamp().alias("effective_date"),
                lit(True).alias("is_current"),
                lit(None).alias("end_date")
            )
            
            # Write to order summary table
            order_summary_df.write.format("delta").mode("overwrite").saveAsTable(self.order_summary_target)
            
            logger.info(f"Successfully generated order summary in {self.order_summary_target}")
            return order_summary_df
        
        except Exception as e:
            logger.error(f"Error generating order summary: {str(e)}")
            raise
    
    def update_order_summary_for_customer_changes(self):
        """
        Update the order summary table when there are changes in the customer table.
        
        Returns:
            Boolean indicating success
        """
        try:
            logger.info("Updating order summary for customer changes")
            
            # Get the current customer and order summary tables
            customer_df = self.spark.table(self.customer_target)
            order_df = self.spark.table(self.order_target)
            
            # Generate new order summary
            new_order_summary = order_df.join(
                customer_df,
                order_df.CustId == customer_df.CustId,
                "inner"
            ).select(
                order_df["*"],
                customer_df.col("Name").alias("CustomerName"),
                customer_df.col("Address"),
                customer_df.col("Phone"),
                customer_df.col("Email"),
                current_timestamp().alias("effective_date"),
                lit(True).alias("is_current"),
                lit(None).alias("end_date")
            )
            
            # Get the order summary delta table
            order_summary_delta = DeltaTable.forName(self.spark, self.order_summary_target)
            
            # Perform SCD Type 2 merge
            order_summary_delta.alias("target").merge(
                new_order_summary.alias("source"),
                "target.OrderId = source.OrderId"
            ).whenMatchedUpdateAll(
                condition="target.CustId <> source.CustId OR " +
                         "target.CustomerName <> source.CustomerName OR " +
                         "target.Address <> source.Address OR " +
                         "target.Phone <> source.Phone OR " +
                         "target.Email <> source.Email"
            ).whenNotMatchedInsertAll().execute()
            
            # Update the is_current and end_date for outdated records
            current_order_summary = self.spark.table(self.order_summary_target)
            
            # Find duplicates (same OrderId but different effective_date)
            window_spec = Window.partitionBy("OrderId").orderBy(col("effective_date").desc())
            ranked_df = current_order_summary.withColumn("rank", rank().over(window_spec))
            
            # Update outdated records
            outdated_records = ranked_df.filter(col("rank") > 1)
            
            if outdated_records.count() > 0:
                order_summary_delta.alias("target").merge(
                    outdated_records.alias("source"),
                    "target.OrderId = source.OrderId AND " +
                    "target.effective_date = source.effective_date"
                ).whenMatchedUpdate(
                    set={
                        "is_current": "False",
                        "end_date": "current_timestamp()"
                    }
                ).execute()
            
            logger.info("Successfully updated order summary for customer changes")
            return True
        
        except Exception as e:
            logger.error(f"Error updating order summary for customer changes: {str(e)}")
            raise

def get_spark_session():
    """
    Get or create a SparkSession.
    
    Returns:
        SparkSession object
    """
    return SparkSession.builder \
        .appName("Customer Order Data Processing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()