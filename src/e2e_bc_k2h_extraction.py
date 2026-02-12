"""
E2E Policy Services - BC K2H Data Extraction

This module implements the data extraction and transformation pipeline for the
E2E Policy Services BC K2H Data Extraction project. It extracts data from SQL Server
source tables, performs necessary transformations including Kanji normalization,
and loads the data into an Oracle staging table.
"""

import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, when, isnull, expr, current_timestamp, udf
)
from pyspark.sql.types import StringType, DateType, TimestampType
from typing import Dict, Any

from kanji_normalization import normalize_kanji_name


def create_spark_session() -> SparkSession:
    """
    Create and configure a Spark session for the ETL job.
    
    Returns:
        SparkSession: Configured Spark session
    """
    return (SparkSession.builder
            .appName("E2E_BC_K2H_DATA_EXTRACTION")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .getOrCreate())


def get_config() -> Dict[str, Any]:
    """
    Get configuration parameters for the ETL job.
    
    Returns:
        dict: Configuration parameters
    """
    # In a real implementation, these would be retrieved from a configuration service,
    # environment variables, or Databricks secrets
    return {
        "source_jdbc_url": os.environ.get(
            "SOURCE_JDBC_URL", 
            "jdbc:sqlserver://txdb-server:1433;databaseName=ZSYSTXMGMT"
        ),
        "source_jdbc_driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "source_jdbc_user": os.environ.get("SOURCE_JDBC_USER", "txdb_reader"),
        "source_jdbc_password": os.environ.get("SOURCE_JDBC_PASSWORD", ""),
        "target_jdbc_url": os.environ.get(
            "TARGET_JDBC_URL", 
            "jdbc:oracle:thin:@//oracle-edw:1521/EDWSTG"
        ),
        "target_jdbc_driver": "oracle.jdbc.driver.OracleDriver",
        "target_jdbc_user": os.environ.get("TARGET_JDBC_USER", "edw_writer"),
        "target_jdbc_password": os.environ.get("TARGET_JDBC_PASSWORD", ""),
        "target_table": "STG_E2E_BC_K2H_TXDB_DATA",
        "jdbc_batch_size": 10000,
        "jdbc_num_partitions": 10
    }


def extract_source_data(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract data from source tables using a custom SQL query that joins the required tables.
    
    Args:
        spark: SparkSession
        config: Configuration parameters
        
    Returns:
        dict: Dictionary containing the extracted DataFrame
    """
    # This SQL implements the Source Qualifier logic that would normally be in $$M_Src_SQL
    # It joins the four source tables and applies the necessary filters
    src_sql = """
    SELECT 
        trp.POLICY_ID AS FNL_POLICY_ID,
        trp.REQUEST_ID AS TR_REQUEST_ID,
        trp.REQUEST_STATUS AS TR_REQUEST_STATUS,
        trp.PUSG_EDT AS TR_UPDATE_DATETIME,
        ttb.REQUEST_ID AS TTB_REQUEST_ID,
        ttb.POLICY_ID AS TTB_POLICY_ID,
        ttb.TRANSACTION_TYPE AS TTB_TRANSACTION_TYPE,
        ttb.TRANSACTION_STATUS AS TTB_TRANSACTION_STATUS,
        ttb.DISCARD_FLAG AS TTB_DISCARD_FLAG,
        ttb.TRANSACTION_ID AS TTB_TRANSACTION_ID,
        trp.TRANSACTION_ID AS TRP_TRANSACTION_ID,
        trp.POLICY_ID AS TRP_POLICY_ID,
        trp.OTHER_POLICY_ID_USE AS TRP_OTHER_POLICY_ID_USE,
        trp.SRC_SYS_MSTR_CD AS TRP_SRC_SYS_MSTR_CD,
        trp.OTHER_POLICY_ID AS TRP_OTHER_POLICY_ID,
        trp.POWN_NAME_NUMBER AS TRP_POWN_NAME_NUMBER,
        ttr.TRANSACTION_ID AS TTR_TRANSACTION_ID,
        ttr.BENEFICIARY_CHANGE_FLAG AS TTR_BENIFICIARY_CHANGE_FLAG,
        ttr.OBJECT_SUB_TYPE AS TTR_OBJECT_SUB_TYPE,
        ttr.TRANSACTION_RELATION_ID AS TTR_TRANSACTION_RELATION_ID,
        ttr.NAME_NUMBER AS TTR_NAME_NUMBER,
        ttdbc.REQUEST_ID AS TTDBC_REQUEST_ID,
        ttdbc.TRANSACTION_RELATION_ID AS TTDBC_TRANSACTION_RELATION_ID,
        ttdbc.CHANGE_STATE AS TTDBC_CHANGE_STATE,
        ttdbc.LASTNAME_KANJI AS TTDBC_LASTNAME_KANJI,
        ttdbc.FIRSTNAME_KANJI AS TTDBC_FIRSTNAME_KANJI,
        ttdbc.GENDER_CODE AS TTDBC_GENDER_CODE,
        ttdbc.LASTNAME AS TTDBC_LASTNAME,
        ttdbc.FIRSTNAME AS TTDBC_FIRSTNAME,
        ttdbc.PERCENTAGE AS TTDBC_PERCENTAGE,
        ttdbc.CREATE_DATETIME AS TTDBC_EFFECTIVE_DATE,
        ttdbc.TRANSACTION_BENEFICIARY_CHANGE_ID AS TTDBC_CHANGE_ID,
        -- The following fields appear to be derived in the source SQL based on the mapping
        -- These are placeholders that would be replaced with actual logic from $$M_Src_SQL
        trp.POWN_LASTNAME AS DATA_TYPE,
        trp.POWN_FIRSTNAME AS POLICY_TYPE,
        trp.POWN_LASTNAME_KANJI AS MC_CRNCY
    FROM 
        T_TX_REQUEST_POLICY trp
    JOIN 
        T_TX_BASIC ttb ON trp.TRANSACTION_ID = ttb.TRANSACTION_ID
    JOIN 
        T_TX_RELATION ttr ON trp.TRANSACTION_ID = ttr.TRANSACTION_ID
    JOIN 
        T_TX_DTL_BENEFICIARY_CHANGE ttdbc ON ttr.TRANSACTION_RELATION_ID = ttdbc.TRANSACTION_RELATION_ID
    WHERE 
        ttr.BENEFICIARY_CHANGE_FLAG = 'Y'
    """
    
    # In a real implementation, we would use the actual $$M_Src_SQL parameter value
    # src_sql = spark.conf.get("M_Src_SQL")
    
    jdbc_properties = {
        "user": config["source_jdbc_user"],
        "password": config["source_jdbc_password"],
        "driver": config["source_jdbc_driver"],
        "fetchsize": "10000"
    }
    
    print("Extracting source data...")
    source_df = spark.read.format("jdbc") \
        .option("url", config["source_jdbc_url"]) \
        .option("query", src_sql) \
        .options(**jdbc_properties) \
        .load()
    
    print(f"Extracted {source_df.count()} records from source")
    return {"source_df": source_df}


def transform_data(spark: SparkSession, dfs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply transformations to the source data including trimming, null handling,
    and Kanji normalization.
    
    Args:
        spark: SparkSession
        dfs: Dictionary containing DataFrames
        
    Returns:
        dict: Dictionary containing the transformed DataFrame
    """
    print("Applying transformations...")
    source_df = dfs["source_df"]
    
    # Register UDFs for Kanji normalization
    normalize_kanji_udf = udf(normalize_kanji_name, StringType())
    
    # Apply transformations equivalent to EXP_DATA_EXTRACTION
    transformed_df = source_df \
        .withColumn("out_TTDBC_LASTNAME_KANJI", 
                   when(isnull(trim(col("TTDBC_LASTNAME_KANJI"))), 
                        lit("")).otherwise(trim(col("TTDBC_LASTNAME_KANJI")))) \
        .withColumn("out_TTDBC_FIRSTNAME_KANJI", 
                   when(isnull(trim(col("TTDBC_FIRSTNAME_KANJI"))), 
                        lit("")).otherwise(trim(col("TTDBC_FIRSTNAME_KANJI")))) \
        .withColumn("out_TTDBC_LASTNAME", trim(col("TTDBC_LASTNAME"))) \
        .withColumn("out_TTDBC_FIRSTNAME", trim(col("TTDBC_FIRSTNAME")))
    
    # Apply Kanji normalization equivalent to JTX_LAST_NAME_KANJI_CONVERSION and JTX_FIRST_NAME_KANJI_CONVERSION
    final_df = transformed_df \
        .withColumn("TTDBC_LASTNAME_KANJI", normalize_kanji_udf(col("out_TTDBC_LASTNAME_KANJI"))) \
        .withColumn("TTDBC_FIRSTNAME_KANJI", normalize_kanji_udf(col("out_TTDBC_FIRSTNAME_KANJI"))) \
        .withColumn("TTDBC_LASTNAME", col("out_TTDBC_LASTNAME")) \
        .withColumn("TTDBC_FIRSTNAME", col("out_TTDBC_FIRSTNAME")) \
        .withColumn("CREATE_DATETIME", current_timestamp()) \
        .drop("out_TTDBC_LASTNAME_KANJI", "out_TTDBC_FIRSTNAME_KANJI", 
              "out_TTDBC_LASTNAME", "out_TTDBC_FIRSTNAME")
    
    # Ensure proper data types for date fields
    final_df = final_df \
        .withColumn("TR_UPDATE_DATETIME", col("TR_UPDATE_DATETIME").cast(DateType())) \
        .withColumn("TTDBC_EFFECTIVE_DATE", col("TTDBC_EFFECTIVE_DATE").cast(DateType())) \
        .withColumn("CREATE_DATETIME", col("CREATE_DATETIME").cast(TimestampType()))
    
    print(f"Transformation complete. Result has {final_df.count()} records")
    return {"final_df": final_df}


def load_data(spark: SparkSession, dfs: Dict[str, Any], config: Dict[str, Any]) -> None:
    """
    Load the transformed data into the target Oracle table.
    
    Args:
        spark: SparkSession
        dfs: Dictionary containing DataFrames
        config: Configuration parameters
    """
    final_df = dfs["final_df"]
    target_table = config["target_table"]
    
    print(f"Loading data to target table {target_table}...")
    
    jdbc_properties = {
        "user": config["target_jdbc_user"],
        "password": config["target_jdbc_password"],
        "driver": config["target_jdbc_driver"]
    }
    
    # Write to Oracle target table in append mode
    final_df.write.format("jdbc") \
        .option("url", config["target_jdbc_url"]) \
        .option("dbtable", target_table) \
        .option("batchsize", config["jdbc_batch_size"]) \
        .option("numPartitions", config["jdbc_num_partitions"]) \
        .options(**jdbc_properties) \
        .mode("append") \
        .save()
    
    print(f"Successfully loaded {final_df.count()} records to {target_table}")


def run_etl_job():
    """
    Main function to run the ETL job.
    """
    print(f"Starting E2E BC K2H Data Extraction job at {datetime.datetime.now()}")
    
    try:
        spark = create_spark_session()
        config = get_config()
        
        # Extract data from source
        dfs = extract_source_data(spark, config)
        
        # Transform data
        dfs = transform_data(spark, dfs)
        
        # Load data to target
        load_data(spark, dfs, config)
        
        print(f"E2E BC K2H Data Extraction job completed successfully at {datetime.datetime.now()}")
        
    except Exception as e:
        print(f"Error in E2E BC K2H Data Extraction job: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    run_etl_job()