"""
Finance Data Processor - Main module

This module implements a PySpark job to extract data from Everest ECC source tables,
apply transformations, and load the data into the Finance table.
"""
import sys
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import SOURCE_PATHS, TARGET_PATH, SPECIAL_CURRENCY_RULES
from src.logger import setup_logger


# Initialize logger
logger = setup_logger("finance_processor")


def create_spark_session() -> SparkSession:
    """
    Create and configure Spark session
    
    Returns:
        Configured SparkSession
    """
    try:
        spark = SparkSession.builder \
            .appName("Finance Data Processor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "8g") \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise


def read_source_data(spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Read source data from ADLS Storage and Hive Metastore
    
    Args:
        spark: SparkSession
        
    Returns:
        Dictionary of DataFrames containing source data
    """
    try:
        logger.info("Reading source data")
        
        # Read FAGLFLEXA data from ADLS Storage
        faglflexa_df = spark.read.parquet(SOURCE_PATHS['FAGLFLEXA'])
        logger.info(f"Read FAGLFLEXA data: {faglflexa_df.count()} rows")
        
        # Read BSEG data from ADLS Storage
        bseg_df = spark.read.parquet(SOURCE_PATHS['BSEG'])
        logger.info(f"Read BSEG data: {bseg_df.count()} rows")
        
        # Read Golden Entity view from Hive Metastore
        golden_entity_df = spark.table(SOURCE_PATHS['GOLDEN_ENTITY'])
        logger.info(f"Read Golden Entity data: {golden_entity_df.count()} rows")
        
        # Read Golden GL view from Hive Metastore
        golden_gl_df = spark.table(SOURCE_PATHS['GOLDEN_GL'])
        logger.info(f"Read Golden GL data: {golden_gl_df.count()} rows")
        
        # Read Golden Trading Partner view from Hive Metastore
        golden_tp_df = spark.table(SOURCE_PATHS['GOLDEN_TRADING_PARTNER'])
        logger.info(f"Read Golden Trading Partner data: {golden_tp_df.count()} rows")
        
        # Read BPC exchange rates from Hive Metastore
        bpc_rates_df = spark.table(SOURCE_PATHS['BPC_EXCHANGE_RATES'])
        logger.info(f"Read BPC exchange rates data: {bpc_rates_df.count()} rows")
        
        return {
            'FAGLFLEXA': faglflexa_df,
            'BSEG': bseg_df,
            'GOLDEN_ENTITY': golden_entity_df,
            'GOLDEN_GL': golden_gl_df,
            'GOLDEN_TRADING_PARTNER': golden_tp_df,
            'BPC_EXCHANGE_RATES': bpc_rates_df
        }
    except Exception as e:
        logger.error(f"Error reading source data: {str(e)}")
        raise


def transform_data(data_dict: Dict[str, DataFrame]) -> DataFrame:
    """
    Apply transformations to the source data
    
    Args:
        data_dict: Dictionary of source DataFrames
        
    Returns:
        Transformed DataFrame
    """
    try:
        logger.info("Starting data transformation")
        
        # Step 1: Filter FAGLFLEXA records where RLDNR = '0L'
        faglflexa_filtered = data_dict['FAGLFLEXA'].filter(F.col("RLDNR") == "0L")
        logger.info(f"Filtered FAGLFLEXA data: {faglflexa_filtered.count()} rows")
        
        # Step 2: Join FAGLFLEXA with BSEG on DOCNR = BELNR, RBUKRS = BUKRS, and RYEAR = GJAHR
        joined_df = faglflexa_filtered.join(
            data_dict['BSEG'],
            (faglflexa_filtered.DOCNR == data_dict['BSEG'].BELNR) &
            (faglflexa_filtered.RBUKRS == data_dict['BSEG'].BUKRS) &
            (faglflexa_filtered.RYEAR == data_dict['BSEG'].GJAHR),
            "inner"
        )
        logger.info(f"Joined FAGLFLEXA and BSEG data: {joined_df.count()} rows")
        
        # Step 3: Derive required fields
        transformed_df = joined_df.withColumn(
            "FiscalYear", F.col("RYEAR")
        ).withColumn(
            "PostingPeriod", F.col("POPER")
        ).withColumn(
            "DocumentNumber", F.col("DOCNR")
        ).withColumn(
            "CompCode", F.col("RBUKRS")
        ).withColumn(
            "PostingDate", F.to_date(F.col("BUDAT"), "yyyyMMdd")
        ).withColumn(
            "DocumentType", F.col("BLART")
        ).withColumn(
            "LocalCurrency", F.col("RHCUR")
        ).withColumn(
            "TransactionCurrency", F.col("RKCUR")
        ).withColumn(
            "LocalCurrencyAmount", F.col("HSL")
        ).withColumn(
            "TransactionCurrencyAmount", F.col("KSL")
        )
        
        # Join with Golden Entity view
        transformed_df = transformed_df.join(
            data_dict['GOLDEN_ENTITY'],
            transformed_df.CompCode == data_dict['GOLDEN_ENTITY'].EntityCode,
            "left"
        ).select(
            transformed_df["*"],
            data_dict['GOLDEN_ENTITY']["EntityName"],
            data_dict['GOLDEN_ENTITY']["EntityRegion"]
        )
        
        # Join with Golden GL view
        transformed_df = transformed_df.join(
            data_dict['GOLDEN_GL'],
            transformed_df.RACCT == data_dict['GOLDEN_GL'].GLCode,
            "left"
        ).select(
            transformed_df["*"],
            data_dict['GOLDEN_GL']["GLName"],
            data_dict['GOLDEN_GL']["GLCategory"]
        )
        
        # Join with Golden Trading Partner view if available
        if "KUNNR" in transformed_df.columns:
            transformed_df = transformed_df.join(
                data_dict['GOLDEN_TRADING_PARTNER'],
                transformed_df.KUNNR == data_dict['GOLDEN_TRADING_PARTNER'].PartnerCode,
                "left"
            ).select(
                transformed_df["*"],
                data_dict['GOLDEN_TRADING_PARTNER']["PartnerName"],
                data_dict['GOLDEN_TRADING_PARTNER']["PartnerType"]
            )
        
        # Step 4: Apply special logic for certain Local Currencies and Transactional Currencies
        special_local_currencies = SPECIAL_CURRENCY_RULES['local_currencies']
        special_transaction_currencies = SPECIAL_CURRENCY_RULES['transaction_currencies']
        
        # Apply exchange rate conversion for special currencies
        transformed_df = transformed_df.withColumn(
            "IsSpecialLocalCurrency", 
            F.col("LocalCurrency").isin(special_local_currencies)
        ).withColumn(
            "IsSpecialTransactionCurrency", 
            F.col("TransactionCurrency").isin(special_transaction_currencies)
        )
        
        # Join with BPC exchange rates for currency conversion
        transformed_df = transformed_df.join(
            data_dict['BPC_EXCHANGE_RATES'],
            (transformed_df.FiscalYear == data_dict['BPC_EXCHANGE_RATES'].FiscalYear) &
            (transformed_df.PostingPeriod == data_dict['BPC_EXCHANGE_RATES'].Period) &
            (transformed_df.LocalCurrency == data_dict['BPC_EXCHANGE_RATES'].FromCurrency) &
            (F.lit("USD") == data_dict['BPC_EXCHANGE_RATES'].ToCurrency),
            "left"
        ).select(
            transformed_df["*"],
            data_dict['BPC_EXCHANGE_RATES']["Rate"].alias("LocalToUSDRate")
        )
        
        # Calculate USD amounts
        transformed_df = transformed_df.withColumn(
            "USDAmount",
            F.when(
                F.col("IsSpecialLocalCurrency"),
                F.col("LocalCurrencyAmount") * F.col("LocalToUSDRate")
            ).otherwise(F.col("LocalCurrencyAmount"))
        )
        
        # Final selection of columns
        result_df = transformed_df.select(
            "FiscalYear",
            "PostingPeriod",
            "PostingDate",
            "DocumentNumber",
            "DocumentType",
            "CompCode",
            "EntityName",
            "EntityRegion",
            "RACCT",
            "GLName",
            "GLCategory",
            F.col("KUNNR").alias("PartnerCode"),
            "PartnerName",
            "PartnerType",
            "LocalCurrency",
            "LocalCurrencyAmount",
            "TransactionCurrency",
            "TransactionCurrencyAmount",
            "USDAmount"
        )
        
        logger.info("Data transformation completed successfully")
        return result_df
    except Exception as e:
        logger.error(f"Error during data transformation: {str(e)}")
        raise


def write_target_data(transformed_df: DataFrame) -> None:
    """
    Write transformed data to target location
    
    Args:
        transformed_df: Transformed DataFrame
    """
    try:
        logger.info(f"Writing data to target location: {TARGET_PATH}")
        
        # Write data to target location
        transformed_df.write \
            .mode("overwrite") \
            .partitionBy("FiscalYear", "PostingPeriod") \
            .parquet(TARGET_PATH)
        
        logger.info(f"Successfully wrote {transformed_df.count()} rows to target location")
    except Exception as e:
        logger.error(f"Error writing data to target location: {str(e)}")
        raise


def run_finance_processor() -> None:
    """
    Main function to run the Finance Data Processor
    """
    try:
        logger.info("Starting Finance Data Processor")
        
        # Create Spark session
        spark = create_spark_session()
        
        # Read source data
        data_dict = read_source_data(spark)
        
        # Transform data
        transformed_df = transform_data(data_dict)
        
        # Write target data
        write_target_data(transformed_df)
        
        logger.info("Finance Data Processor completed successfully")
    except Exception as e:
        logger.error(f"Finance Data Processor failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    run_finance_processor()