"""
Order Processing Module for Delta Lake Data Pipeline

This module contains functionality to process customer and order data,
loading them into Delta tables and creating an order summary with SCD Type 2.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrderProcessor:
    """Class to handle order data processing pipeline."""
    
    def __init__(self, spark):
        """
        Initialize the OrderProcessor with a SparkSession.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        
        # Source paths
        self.customer_source_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
        self.order_source_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
        
        # Target tables
        self.customer_table = "gen_ai_poc_databrickscoe.sdlc_wizard.customer"
        self.order_table = "gen_ai_poc_databrickscoe.sdlc_wizard.order"
        self.order_summary_table = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"

    def read_customer_data(self):
        """
        Read customer data from CSV source.
        
        Returns:
            DataFrame: Customer data
        """
        try:
            logger.info(f"Reading customer data from {self.customer_source_path}")
            customer_df = self.spark.read \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .csv(self.customer_source_path)
            
            logger.info(f"Successfully read customer data with {customer_df.count()} rows")
            return customer_df
        except Exception as e:
            logger.error(f"Error reading customer data: {str(e)}")
            raise

    def read_order_data(self):
        """
        Read order data from CSV source.
        
        Returns:
            DataFrame: Order data
        """
        try:
            logger.info(f"Reading order data from {self.order_source_path}")
            order_df = self.spark.read \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .csv(self.order_source_path)
            
            logger.info(f"Successfully read order data with {order_df.count()} rows")
            return order_df
        except Exception as e:
            logger.error(f"Error reading order data: {str(e)}")
            raise

    def clean_data(self, df, key_columns):
        """
        Clean data by removing nulls in key columns and duplicates.
        
        Args:
            df: DataFrame to clean
            key_columns: List of columns to check for nulls
            
        Returns:
            DataFrame: Cleaned DataFrame
        """
        try:
            logger.info("Cleaning data - removing nulls and duplicates")
            
            # Remove rows with nulls in key columns
            for col_name in key_columns:
                df = df.filter(col(col_name).isNotNull())
            
            # Remove duplicates
            df = df.dropDuplicates()
            
            logger.info(f"Data cleaning complete. Remaining rows: {df.count()}")
            return df
        except Exception as e:
            logger.error(f"Error cleaning data: {str(e)}")
            raise

    def save_to_delta(self, df, table_name):
        """
        Save DataFrame to Delta table.
        
        Args:
            df: DataFrame to save
            table_name: Target Delta table name
        """
        try:
            logger.info(f"Saving data to Delta table: {table_name}")
            
            # Add processing timestamp
            df_with_ts = df.withColumn("processing_timestamp", current_timestamp())
            
            # Write to Delta table
            df_with_ts.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(table_name)
            
            logger.info(f"Successfully saved data to {table_name}")
        except Exception as e:
            logger.error(f"Error saving to Delta table {table_name}: {str(e)}")
            raise

    def join_customer_order_data(self):
        """
        Join customer and order data.
        
        Returns:
            DataFrame: Joined customer and order data
        """
        try:
            logger.info("Joining customer and order data")
            
            # Read from Delta tables
            customer_df = self.spark.table(self.customer_table)
            order_df = self.spark.table(self.order_table)
            
            # Join data
            joined_df = order_df.join(
                customer_df,
                order_df.CustId == customer_df.CustId,
                "inner"
            )
            
            logger.info(f"Successfully joined data with {joined_df.count()} rows")
            return joined_df
        except Exception as e:
            logger.error(f"Error joining customer and order data: {str(e)}")
            raise

    def create_order_summary_if_not_exists(self):
        """Create order summary table if it doesn't exist."""
        try:
            # Check if table exists
            tables = self.spark.sql(f"SHOW TABLES IN gen_ai_poc_databrickscoe.sdlc_wizard").collect()
            table_exists = any(row.tableName == "ordersummary" for row in tables)
            
            if not table_exists:
                logger.info("Creating order summary table")
                
                # Define schema
                schema = StructType([
                    StructField("OrderId", StringType(), False),
                    StructField("CustId", StringType(), False),
                    StructField("CustomerName", StringType(), True),
                    StructField("OrderAmount", DoubleType(), True),
                    StructField("OrderDate", TimestampType(), True),
                    StructField("StartDate", TimestampType(), False),
                    StructField("EndDate", TimestampType(), True),
                    StructField("IsActive", BooleanType(), False)
                ])
                
                # Create empty DataFrame with schema
                empty_df = self.spark.createDataFrame([], schema)
                
                # Save as Delta table
                empty_df.write \
                    .format("delta") \
                    .saveAsTable(self.order_summary_table)
                
                logger.info("Successfully created order summary table")
            else:
                logger.info("Order summary table already exists")
        except Exception as e:
            logger.error(f"Error creating order summary table: {str(e)}")
            raise

    def update_order_summary_scd2(self, source_df):
        """
        Update order summary table using SCD Type 2.
        
        Args:
            source_df: Source DataFrame with joined customer and order data
        """
        try:
            logger.info("Updating order summary table using SCD Type 2")
            
            # Prepare source data
            source = source_df.select(
                col("OrderId"),
                col("order.CustId").alias("CustId"),
                col("CustomerName"),
                col("OrderAmount"),
                col("OrderDate")
            ).withColumn("StartDate", current_timestamp()) \
             .withColumn("EndDate", lit(None).cast(TimestampType())) \
             .withColumn("IsActive", lit(True))
            
            # Check if target table exists and has data
            if DeltaTable.isDeltaTable(self.spark, f"gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"):
                # Get Delta table
                target_delta_table = DeltaTable.forName(self.spark, self.order_summary_table)
                
                # Perform merge operation (SCD Type 2)
                target_delta_table.alias("target").merge(
                    source.alias("source"),
                    "target.OrderId = source.OrderId"
                ).whenMatchedAndExpr(
                    """
                    target.IsActive = true AND
                    (target.CustId <> source.CustId OR
                     target.CustomerName <> source.CustomerName OR
                     target.OrderAmount <> source.OrderAmount OR
                     target.OrderDate <> source.OrderDate)
                    """
                ).updateExpr(
                    {
                        "EndDate": "current_timestamp()",
                        "IsActive": "false"
                    }
                ).whenNotMatchedInsertAll() \
                .execute()
                
                # Insert new records for updated rows
                updated_condition = """
                    target.OrderId = source.OrderId AND
                    target.IsActive = false AND
                    target.EndDate = current_timestamp()
                """
                
                # Get updated records
                updated_records = self.spark.sql(f"""
                    SELECT source.OrderId, source.CustId, source.CustomerName, 
                           source.OrderAmount, source.OrderDate, 
                           current_timestamp() as StartDate, 
                           NULL as EndDate, 
                           true as IsActive
                    FROM {self.order_summary_table} target
                    JOIN (
                        SELECT * FROM {source.createOrReplaceTempView("source_view")}
                    ) source
                    ON {updated_condition}
                """)
                
                # Insert updated records
                if updated_records.count() > 0:
                    updated_records.write \
                        .format("delta") \
                        .mode("append") \
                        .saveAsTable(self.order_summary_table)
            else:
                # If table doesn't exist or is empty, just insert all records
                source.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(self.order_summary_table)
            
            logger.info("Successfully updated order summary table")
        except Exception as e:
            logger.error(f"Error updating order summary table: {str(e)}")
            raise

    def run_pipeline(self):
        """Run the complete data processing pipeline."""
        try:
            logger.info("Starting order processing pipeline")
            
            # Step 1: Read source data
            customer_df = self.read_customer_data()
            order_df = self.read_order_data()
            
            # Step 2: Clean data
            customer_df_clean = self.clean_data(customer_df, ["CustId", "CustomerName"])
            order_df_clean = self.clean_data(order_df, ["OrderId", "CustId"])
            
            # Save to Delta tables
            self.save_to_delta(customer_df_clean, self.customer_table)
            self.save_to_delta(order_df_clean, self.order_table)
            
            # Step 3: Create order summary table if not exists
            self.create_order_summary_if_not_exists()
            
            # Step 4: Join customer and order data
            joined_df = self.join_customer_order_data()
            
            # Step 5: Update order summary table using SCD Type 2
            self.update_order_summary_scd2(joined_df)
            
            logger.info("Order processing pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

def get_spark_session():
    """
    Create and configure a Spark session.
    
    Returns:
        SparkSession: Configured Spark session
    """
    try:
        spark = SparkSession.builder \
            .appName("Order Processing Pipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        raise

def main():
    """Main entry point for the application."""
    try:
        # Get Spark session
        spark = get_spark_session()
        
        # Create and run pipeline
        processor = OrderProcessor(spark)
        processor.run_pipeline()
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()