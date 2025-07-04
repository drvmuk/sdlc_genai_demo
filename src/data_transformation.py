"""
Module for transforming and joining data.
"""
from pyspark.sql import DataFrame

from src.config import JOINED_DATA_PATH
from src.logging_utils import log_info, log_error, log_start_job, log_end_job

def join_customer_order_data(customer_df: DataFrame, order_df: DataFrame) -> DataFrame:
    """
    Join customer and order data on CustId.
    
    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame
        
    Returns:
        DataFrame: Joined DataFrame
    """
    log_start_job("Join Customer and Order Data")
    
    try:
        # Join customer and order data
        joined_df = customer_df.join(
            order_df,
            customer_df.CustId == order_df.CustId,
            "inner"
        ).select(
            customer_df.CustId,
            customer_df.FirstName,
            customer_df.LastName,
            customer_df.Email,
            customer_df.Phone,
            customer_df.Address,
            customer_df.City,
            customer_df.State,
            customer_df.ZipCode,
            order_df.OrderId,
            order_df.OrderDate,
            order_df.ShipDate,
            order_df.OrderTotal,
            order_df.OrderStatus
        )
        
        # Save joined data
        joined_df.write.mode("overwrite").parquet(JOINED_DATA_PATH)
        log_info(f"Saved joined data to {JOINED_DATA_PATH}")
        
        log_info(f"Joined {joined_df.count()} records")
        log_end_job("Join Customer and Order Data")
        
        return joined_df
    
    except Exception as e:
        log_error("Error joining customer and order data", e)
        raise