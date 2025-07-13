"""
Module for loading CSV data into Delta tables and performing transformations
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, expr, sum as spark_sum
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DeltaTablesLoader:
    def __init__(self, spark=None):
        """Initialize the DeltaTablesLoader with a SparkSession"""
        self.spark = spark or SparkSession.builder.appName("DeltaTablesLoader").getOrCreate()
        
    def load_csv_to_delta(self, source_path, target_table, mode="overwrite"):
        """
        Load CSV data into Delta table
        
        Args:
            source_path: Path to the source CSV file
            target_table: Target Delta table name (fully qualified)
            mode: Write mode (overwrite, append, etc.)
        """
        try:
            logger.info(f"Loading CSV data from {source_path} to {target_table}")
            
            # Read CSV data with header and infer schema
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            
            # Write to Delta table
            df.write.format("delta").mode(mode).saveAsTable(target_table)
            
            logger.info(f"Successfully loaded data into {target_table}")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV data: {str(e)}")
            raise
    
    def transform_delta_tables(self):
        """
        Transform customer_dlt and order_dlt tables:
        - Add TotalAmount column to order_dlt
        - Remove null records
        - Remove duplicate records
        """
        try:
            logger.info("Starting Delta table transformations")
            
            # Transform order_dlt table
            order_df = self.spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.order_dlt")
            
            # Add TotalAmount column
            order_df = order_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
            
            # Remove null records
            order_df = order_df.filter(
                col("OrderId").isNotNull() & 
                col("CustId").isNotNull() & 
                col("PricePerUnit").isNotNull() & 
                col("Qty").isNotNull()
            )
            
            # Remove duplicate records
            order_df = order_df.dropDuplicates(["OrderId"])
            
            # Write back to Delta table
            order_df.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.order_dlt")
            
            # Transform customer_dlt table
            customer_df = self.spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.customer_dlt")
            
            # Remove null records
            customer_df = customer_df.filter(
                col("CustId").isNotNull() & 
                col("Name").isNotNull()
            )
            
            # Remove duplicate records
            customer_df = customer_df.dropDuplicates(["CustId"])
            
            # Write back to Delta table
            customer_df.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customer_dlt")
            
            logger.info("Delta table transformations completed successfully")
            return order_df, customer_df
        except Exception as e:
            logger.error(f"Error transforming Delta tables: {str(e)}")
            raise
    
    def create_order_summary(self):
        """
        Create ordersummary_dlt table by joining customer_dlt and order_dlt tables
        """
        try:
            logger.info("Creating order summary table")
            
            # Read customer and order tables
            customer_df = self.spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.customer_dlt")
            order_df = self.spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.order_dlt")
            
            # Join tables
            order_summary_df = customer_df.join(
                order_df,
                customer_df.CustId == order_df.CustId,
                "inner"
            ).select(
                customer_df.CustId,
                customer_df.Name,
                order_df.OrderId,
                order_df.Date,
                order_df.PricePerUnit,
                order_df.Qty,
                order_df.TotalAmount,
                lit(True).alias("IsActive"),
                current_timestamp().alias("StartDate"),
                lit(None).cast("timestamp").alias("EndDate")
            )
            
            # Write to Delta table
            order_summary_df.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary_dlt")
            
            logger.info("Order summary table created successfully")
            return order_summary_df
        except Exception as e:
            logger.error(f"Error creating order summary table: {str(e)}")
            raise
    
    def implement_scd_type2(self):
        """
        Implement SCD Type 2 logic for ordersummary_dlt table
        """
        try:
            logger.info("Implementing SCD Type 2 logic")
            
            # Get Delta tables
            customer_delta = DeltaTable.forName(self.spark, "gen_ai_poc_databrickscoe.sdlc_wizard.customer_dlt")
            order_summary_delta = DeltaTable.forName(self.spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary_dlt")
            
            # Get the current data
            customer_df = customer_delta.toDF()
            order_summary_df = order_summary_delta.toDF()
            
            # Create a new version of the data (simulating changes in customer data)
            updated_customer_df = self.spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.customer_dlt")
            order_df = self.spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.order_dlt")
            
            # Join to create new version of order summary
            new_order_summary_df = updated_customer_df.join(
                order_df,
                updated_customer_df.CustId == order_df.CustId,
                "inner"
            ).select(
                updated_customer_df.CustId,
                updated_customer_df.Name,
                order_df.OrderId,
                order_df.Date,
                order_df.PricePerUnit,
                order_df.Qty,
                order_df.TotalAmount,
                lit(True).alias("IsActive"),
                current_timestamp().alias("StartDate"),
                lit(None).cast("timestamp").alias("EndDate")
            )
            
            # Identify changes (in a real scenario, we would compare with previous version)
            # For this example, we'll mark all existing records as inactive and insert new ones
            
            # Mark existing records as inactive
            order_summary_delta.update(
                condition=expr("IsActive = true"),
                set={
                    "IsActive": "false",
                    "EndDate": "current_timestamp()"
                }
            )
            
            # Insert new records
            new_order_summary_df.write.format("delta").mode("append").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary_dlt")
            
            logger.info("SCD Type 2 implementation completed successfully")
            return new_order_summary_df
        except Exception as e:
            logger.error(f"Error implementing SCD Type 2: {str(e)}")
            raise
    
    def create_customer_aggregate_spend(self):
        """
        Create customeraggregatespend_dlt table with aggregated data from ordersummary_dlt
        """
        try:
            logger.info("Creating customer aggregate spend table")
            
            # Read order summary table
            order_summary_df = self.spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary_dlt")
            
            # Filter only active records
            active_orders = order_summary_df.filter(col("IsActive") == True)
            
            # Aggregate data
            aggregate_df = active_orders.groupBy("Name", "Date").agg(
                spark_sum("TotalAmount").alias("TotalSpend")
            )
            
            # Write to Delta table
            aggregate_df.write.format("delta").mode("overwrite").saveAsTable("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend_dlt")
            
            logger.info("Customer aggregate spend table created successfully")
            return aggregate_df
        except Exception as e:
            logger.error(f"Error creating customer aggregate spend table: {str(e)}")
            raise