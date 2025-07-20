"""
Module for creating and maintaining the customer aggregate spend table.
Implements TR-ORD-006 and TR-ORD-007.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, max, min, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

from src.config import ORDER_SUMMARY_TABLE, CUSTOMER_AGGREGATE_SPEND_TABLE, IS_CURRENT_COL
from src.logger import get_logger, log_job_start, log_job_end

logger = get_logger(__name__)

def create_spark_session():
    """Create and return a SparkSession"""
    return SparkSession.builder \
        .appName("Customer Aggregate Processor") \
        .getOrCreate()

def create_customer_aggregate_spend_table_if_not_exists(spark):
    """
    Create the customer aggregate spend table if it doesn't exist
    
    Args:
        spark: SparkSession
    """
    logger.info(f"Checking if table {CUSTOMER_AGGREGATE_SPEND_TABLE} exists")
    
    # Check if the table exists
    tables = spark.sql(f"SHOW TABLES IN {'.'.join(CUSTOMER_AGGREGATE_SPEND_TABLE.split('.')[:2])}")
    table_exists = tables.filter(col("tableName") == CUSTOMER_AGGREGATE_SPEND_TABLE.split('.')[-1]).count() > 0
    
    if not table_exists:
        logger.info(f"Creating table {CUSTOMER_AGGREGATE_SPEND_TABLE}")
        
        # Define schema for the customer aggregate spend table
        schema = StructType([
            StructField("CustId", StringType(), False),
            StructField("CustomerName", StringType(), True),
            StructField("TotalSpend", DoubleType(), True),
            StructField("OrderCount", IntegerType(), True),
            StructField("AverageOrderAmount", DoubleType(), True),
            StructField("MaxOrderAmount", DoubleType(), True),
            StructField("MinOrderAmount", DoubleType(), True),
            StructField("last_updated", TimestampType(), False)
        ])
        
        # Create empty DataFrame with the defined schema
        empty_df = spark.createDataFrame([], schema)
        
        # Write the empty DataFrame as a Delta table
        empty_df.write.format("delta").saveAsTable(CUSTOMER_AGGREGATE_SPEND_TABLE)
        logger.info(f"Successfully created table {CUSTOMER_AGGREGATE_SPEND_TABLE}")
    else:
        logger.info(f"Table {CUSTOMER_AGGREGATE_SPEND_TABLE} already exists")

def load_customer_aggregate_spend_data(spark):
    """
    Aggregate TotalAmount from order summary table and load it into customer aggregate spend table
    
    Args:
        spark: SparkSession
    """
    logger.info("Loading customer aggregate spend data")
    
    try:
        # Read order summary data - only current records
        order_summary_df = spark.read.format("delta").table(ORDER_SUMMARY_TABLE) \
            .filter(col(IS_CURRENT_COL) == True)
        
        # Aggregate data by customer
        aggregated_df = order_summary_df.groupBy("CustId", "CustomerName") \
            .agg(
                sum("TotalAmount").alias("TotalSpend"),
                count("OrderId").alias("OrderCount"),
                avg("TotalAmount").alias("AverageOrderAmount"),
                max("TotalAmount").alias("MaxOrderAmount"),
                min("TotalAmount").alias("MinOrderAmount")
            ) \
            .withColumn("last_updated", current_timestamp())
        
        # Write to customer aggregate spend table
        aggregated_df.write.format("delta").mode("overwrite").saveAsTable(CUSTOMER_AGGREGATE_SPEND_TABLE)
        logger.info(f"Successfully loaded data to {CUSTOMER_AGGREGATE_SPEND_TABLE}")
        
    except Exception as e:
        logger.error(f"Error loading customer aggregate spend data: {str(e)}")
        raise

def main():
    """Main function to create and maintain the customer aggregate spend table"""
    job_name = "Customer Aggregate Spend Processing"
    log_job_start(logger, job_name)
    
    try:
        spark = create_spark_session()
        
        # Create customer aggregate spend table if it doesn't exist (TR-ORD-006)
        create_customer_aggregate_spend_table_if_not_exists(spark)
        
        # Load customer aggregate spend data (TR-ORD-007)
        load_customer_aggregate_spend_data(spark)
        
        log_job_end(logger, job_name)
        
    except Exception as e:
        logger.error(f"Error in {job_name}: {str(e)}")
        raise

if __name__ == "__main__":
    main()