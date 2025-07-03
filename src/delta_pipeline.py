"""
Delta Lake Pipeline for Customer and Order data processing
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from typing import Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DeltaPipeline:
    """
    Pipeline for processing customer and order data into Delta tables
    and creating a summary table with SCD Type 2
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the pipeline with a SparkSession
        
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
        self.summary_table = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"

    def read_source_data(self) -> Tuple[DataFrame, DataFrame]:
        """
        Read customer and order data from source CSV files
        
        Returns:
            Tuple of (customer_df, order_df)
        """
        try:
            logger.info("Reading customer data from %s", self.customer_source_path)
            customer_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(self.customer_source_path)
            
            logger.info("Reading order data from %s", self.order_source_path)
            order_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(self.order_source_path)
            
            return customer_df, order_df
            
        except Exception as e:
            logger.error("Error reading source data: %s", str(e))
            raise
    
    def clean_customer_data(self, customer_df: DataFrame) -> DataFrame:
        """
        Clean customer data by removing nulls and duplicates
        
        Args:
            customer_df: Raw customer DataFrame
            
        Returns:
            Cleaned customer DataFrame
        """
        try:
            logger.info("Cleaning customer data")
            
            # Remove rows with null values in key fields
            cleaned_df = customer_df.filter(
                (F.col("CustId").isNotNull()) & 
                (F.col("Name").isNotNull())
            )
            
            # Drop duplicates based on CustId
            cleaned_df = cleaned_df.dropDuplicates(["CustId"])
            
            # Add audit columns
            cleaned_df = cleaned_df.withColumn("ProcessedTimestamp", F.current_timestamp())
            
            return cleaned_df
            
        except Exception as e:
            logger.error("Error cleaning customer data: %s", str(e))
            raise
    
    def clean_order_data(self, order_df: DataFrame) -> DataFrame:
        """
        Clean order data by removing nulls and duplicates
        
        Args:
            order_df: Raw order DataFrame
            
        Returns:
            Cleaned order DataFrame
        """
        try:
            logger.info("Cleaning order data")
            
            # Remove rows with null values in key fields
            cleaned_df = order_df.filter(
                (F.col("OrderId").isNotNull()) & 
                (F.col("CustId").isNotNull()) &
                (F.col("ItemName").isNotNull())
            )
            
            # Drop duplicates based on OrderId
            cleaned_df = cleaned_df.dropDuplicates(["OrderId"])
            
            # Calculate total amount
            cleaned_df = cleaned_df.withColumn(
                "TotalAmount", 
                F.col("PricePerUnit") * F.col("Qty")
            )
            
            # Add audit columns
            cleaned_df = cleaned_df.withColumn("ProcessedTimestamp", F.current_timestamp())
            
            return cleaned_df
            
        except Exception as e:
            logger.error("Error cleaning order data: %s", str(e))
            raise
    
    def save_to_delta(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        """
        Save DataFrame to Delta table
        
        Args:
            df: DataFrame to save
            table_name: Target Delta table name
            mode: Write mode (overwrite, append, etc.)
        """
        try:
            logger.info(f"Saving data to Delta table: {table_name}")
            
            df.write \
                .format("delta") \
                .mode(mode) \
                .saveAsTable(table_name)
                
            logger.info(f"Successfully saved data to {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving to Delta table {table_name}: {str(e)}")
            raise
    
    def create_summary_scd2(self, customer_df: DataFrame, order_df: DataFrame) -> None:
        """
        Create summary table with SCD Type 2 implementation
        
        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame
        """
        try:
            logger.info("Creating summary table with SCD Type 2")
            
            # Join customer and order data
            joined_df = order_df.join(
                customer_df,
                on="CustId",
                how="inner"
            )
            
            # Create summary with additional columns for SCD Type 2
            summary_df = joined_df.select(
                "OrderId",
                "CustId",
                "Name",
                "EmailId",
                "Region",
                "ItemName",
                "PricePerUnit",
                "Qty",
                "TotalAmount",
                "Date",
                F.current_timestamp().alias("EffectiveStartDate"),
                F.lit(None).cast("timestamp").alias("EffectiveEndDate"),
                F.lit(True).alias("IsCurrent")
            )
            
            summary_df.createOrReplaceTempView("source_view") #Added to fix issue at line 230

            # Check if summary table exists
            tables = self.spark.sql("SHOW TABLES IN gen_ai_poc_databrickscoe.sdlc_wizard").collect()
            table_exists = any(row.tableName == "ordersummary" for row in tables)
            
            if table_exists:
                # Implement SCD Type 2 logic for existing table
                summary_table = DeltaTable.forName(self.spark, self.summary_table)
                
                # Identify new and changed records
                matched_updates = summary_table.alias("target").merge(
                    summary_df.alias("source"),
                    "target.OrderId = source.OrderId AND target.IsCurrent = true"
                )
                
                # Update existing records (expire them)
                matched_updates.whenMatchedUpdate(
                    condition="target.Name <> source.Name OR target.EmailId <> source.EmailId OR target.Region <> source.Region",
                    set={
                        "EffectiveEndDate": F.current_timestamp(),
                        "IsCurrent": F.lit(False)
                    }
                )
                
                # Insert new records
                matched_updates.whenNotMatchedInsertAll()
                
                # Execute the merge
                matched_updates.execute()
                
                # Insert new versions of changed records
                changed_records = self.spark.sql(f"""
                    SELECT source.*
                    FROM {self.summary_table} target
                    JOIN source_view source
                    ON target.OrderId = source.OrderId
                    WHERE target.EffectiveEndDate IS NOT NULL
                    AND target.IsCurrent = false
                """)

                changed_records = changed_records.dropDuplicates(["CustId","Name","EmailId","Region"]) # Added to fix duplicate insert issue in ordersummary table
                
                if changed_records.count() > 0:
                    changed_records.write.format("delta").mode("append").saveAsTable(self.summary_table)
                
            else:
                # First time load
                self.save_to_delta(summary_df, self.summary_table)
                
            logger.info("Successfully created/updated summary table with SCD Type 2")
            
        except Exception as e:
            logger.error(f"Error creating summary table: {str(e)}")
            raise
    
    def run_pipeline(self) -> None:
        """
        Execute the full pipeline
        """
        try:
            logger.info("Starting Delta pipeline execution")
            
            # Read source data
            customer_df, order_df = self.read_source_data()
            
            # Clean data
            cleaned_customer_df = self.clean_customer_data(customer_df)
            cleaned_order_df = self.clean_order_data(order_df)
            
            # Save cleaned data to Delta tables
            self.save_to_delta(cleaned_customer_df, self.customer_table)
            self.save_to_delta(cleaned_order_df, self.order_table)
            
            # Create summary table with SCD Type 2
            self.create_summary_scd2(cleaned_customer_df, cleaned_order_df)
            
            logger.info("Delta pipeline execution completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise


def get_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession
    
    Returns:
        Configured SparkSession
    """
    return SparkSession.builder \
        .appName("Customer Order Delta Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def main():
    """
    Main entry point for the pipeline
    """
    try:
        # Get SparkSession
        spark = get_spark_session()
        
        # Initialize and run pipeline
        pipeline = DeltaPipeline(spark)
        pipeline.run_pipeline()
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise
    finally:
        logger.info("Pipeline execution completed")


if __name__ == "__main__":
    main()
