"""
Module for managing Slowly Changing Dimension (SCD) Type 2 logic.
"""
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

from src.config import FULL_TABLE_NAME
from src.logging_utils import log_info, log_error, log_start_job, log_end_job

def get_spark_session() -> SparkSession:
    """
    Get or create a Spark session.
    
    Returns:
        SparkSession: Active Spark session
    """
    return SparkSession.builder.getOrCreate()

def prepare_data_for_scd(joined_df: DataFrame) -> DataFrame:
    """
    Prepare data for SCD Type 2 loading by adding IsActive, StartDate, and EndDate columns.
    
    Args:
        joined_df: Joined DataFrame
        
    Returns:
        DataFrame: DataFrame prepared for SCD Type 2 loading
    """
    # Add SCD Type 2 columns
    current_time = current_timestamp()
    
    prepared_df = joined_df.withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", current_time) \
        .withColumn("EndDate", lit(None))
    
    return prepared_df

def load_scd_type2(joined_df: DataFrame):
    """
    Load joined data into ordersummary table using SCD Type 2 logic.
    
    Args:
        joined_df: Joined DataFrame
    """
    log_start_job("Load Data using SCD Type 2 Logic")
    spark = get_spark_session()
    
    try:
        # Prepare data for SCD Type 2 loading
        prepared_df = prepare_data_for_scd(joined_df)
        
        # Register DataFrame as temporary view
        prepared_df.createOrReplaceTempView("source_data")
        
        # Perform merge operation using Delta Lake
        merge_sql = f"""
        MERGE INTO {FULL_TABLE_NAME} target
        USING source_data source
        ON target.CustId = source.CustId AND target.OrderId = source.OrderId
        WHEN MATCHED AND (
            target.FirstName != source.FirstName OR
            target.LastName != source.LastName OR
            target.Email != source.Email OR
            target.Phone != source.Phone OR
            target.Address != source.Address OR
            target.City != source.City OR
            target.State != source.State OR
            target.ZipCode != source.ZipCode OR
            target.OrderStatus != source.OrderStatus OR
            target.OrderTotal != source.OrderTotal OR
            target.ShipDate != source.ShipDate
        ) AND target.IsActive = true THEN
            UPDATE SET
                IsActive = false,
                EndDate = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (
                CustId, FirstName, LastName, Email, Phone, Address, City, State, ZipCode,
                OrderId, OrderDate, ShipDate, OrderTotal, OrderStatus, IsActive, StartDate, EndDate
            )
            VALUES (
                source.CustId, source.FirstName, source.LastName, source.Email, source.Phone,
                source.Address, source.City, source.State, source.ZipCode, source.OrderId,
                source.OrderDate, source.ShipDate, source.OrderTotal, source.OrderStatus,
                true, current_timestamp(), NULL
            )
        """
        
        spark.sql(merge_sql)
        
        # Insert new records for updated rows
        insert_sql = f"""
        INSERT INTO {FULL_TABLE_NAME}
        SELECT
            source.CustId, source.FirstName, source.LastName, source.Email, source.Phone,
            source.Address, source.City, source.State, source.ZipCode, source.OrderId,
            source.OrderDate, source.ShipDate, source.OrderTotal, source.OrderStatus,
            true, current_timestamp(), NULL
        FROM source_data source
        JOIN {FULL_TABLE_NAME} target
        ON target.CustId = source.CustId AND target.OrderId = source.OrderId
        WHERE target.IsActive = false AND target.EndDate = current_timestamp()
        """
        
        spark.sql(insert_sql)
        
        log_info(f"Successfully loaded data into {FULL_TABLE_NAME} using SCD Type 2 logic")
        log_end_job("Load Data using SCD Type 2 Logic")
    
    except Exception as e:
        log_error("Error loading data using SCD Type 2 logic", e)
        raise

def automate_scd_updates(updated_customer_df: DataFrame, order_df: DataFrame):
    """
    Automate updates to ordersummary table based on changes in customer data.
    
    Args:
        updated_customer_df: Updated customer DataFrame
        order_df: Order DataFrame
    """
    log_start_job("Automate SCD Updates")
    
    try:
        # Join updated customer data with order data
        from src.data_transformation import join_customer_order_data
        joined_df = join_customer_order_data(updated_customer_df, order_df)
        
        # Load joined data using SCD Type 2 logic
        load_scd_type2(joined_df)
        
        log_end_job("Automate SCD Updates")
    
    except Exception as e:
        log_error("Error automating SCD updates", e)
        raise