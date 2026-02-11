"""
Data loading module for E2E Policy Services.
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict
from .config import DB_CONFIG, TARGET_TABLE, BATCH_SIZE

def load_to_target(spark: SparkSession, data: DataFrame) -> None:
    """
    Load transformed data to the target table.
    
    Args:
        spark: SparkSession
        data: DataFrame to load
        
    Returns:
        None
    """
    target_config = DB_CONFIG["target"]
    
    # Write data to target table
    (data.write
     .format("jdbc")
     .option("url", target_config["jdbc_url"])
     .option("dbtable", TARGET_TABLE)
     .option("user", target_config["user"])
     .option("password", target_config["password"])
     .option("driver", target_config["driver"])
     .option("batchsize", BATCH_SIZE)
     .mode("append")
     .save())
    
    print(f"Successfully loaded {data.count()} records to {TARGET_TABLE}")