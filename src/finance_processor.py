"""
Finance Data Processor - Main module for transforming and loading finance data
from ECC Everest source system into the target Finance table.
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType

from config import (
    SOURCE_TABLES, TARGET_TABLE, LOCAL_CURRENCIES, 
    TRANSACTIONAL_CURRENCIES, EMAIL_CONFIG, LOG_CONFIG
)

class FinanceDataProcessor:
    """
    Class to process finance data from ECC Everest source system
    """
    
    def __init__(self, spark: SparkSession, fiscal_year: str, posting_period: str):
        """
        Initialize the Finance Data Processor
        
        Args:
            spark: SparkSession instance
            fiscal_year: Fiscal year to process
            posting_period: Posting period to process
        """
        self.spark = spark
        self.fiscal_year = fiscal_year
        self.posting_period = posting_period
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Set up and configure logger"""
        logger = logging.getLogger("FinanceDataProcessor")
        logger.setLevel(getattr(logging, LOG_CONFIG["log_level"]))
        
        # Create log directory if it doesn't exist
        log_dir = LOG_CONFIG["log_path"]
        os.makedirs(log_dir, exist_ok=True)
        
        # Create file handler
        log_file = f"{log_dir}/finance_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)
        
        # Create formatter and add it to the handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(file_handler)
        
        return logger
    
    def _read_source_data(self) -> Dict[str, DataFrame]:
        """
        Read data from source tables
        
        Returns:
            Dictionary containing DataFrames for each source table
        """
        self.logger.info("Reading source data")
        
        try:
            # Read FAGLFLEXA with filter RLDNR = '0L'
            faglflexa_df = self.spark.read.table(SOURCE_TABLES["FAGLFLEXA"]) \
                .filter(F.col("RLDNR") == "0L")
            
            # Read BSEG with filter XBILK = 'X'
            bseg_df = self.spark.read.table(SOURCE_TABLES["BSEG"]) \
                .filter(F.col("XBILK") == "X")
                
            # Read golden views and other reference tables
            entity_view = self.spark.read.table(SOURCE_TABLES["ENTITY_VIEW"])
            gl_view = self.spark.read.table(SOURCE_TABLES["GL_VIEW"])
            trading_partner_view = self.spark.read.table(SOURCE_TABLES["TRADING_PARTNER_VIEW"])
            exchange_rate_df = self.spark.read.table(SOURCE_TABLES["EXCHANGE_RATE"])
            revenue_entity_df = self.spark.read.table(SOURCE_TABLES["REVENUE_ENTITY"])
            
            self.logger.info(f"Successfully read source data. FAGLFLEXA rows: {faglflexa_df.count()}, BSEG rows: {bseg_df.count()}")
            
            return {
                "FAGLFLEXA": faglflexa_df,
                "BSEG": bseg_df,
                "ENTITY_VIEW": entity_view,
                "GL_VIEW": gl_view,
                "TRADING_PARTNER_VIEW": trading_partner_view,
                "EXCHANGE_RATE": exchange_rate_df,
                "REVENUE_ENTITY": revenue_entity_df
            }
            
        except Exception as e:
            self.logger.error(f"Error reading source data: {str(e)}")
            self._send_error_notification(f"Error reading source data: {str(e)}")
            raise
    
    def _join_source_tables(self, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Join FAGLFLEXA and BSEG tables on specified fields
        
        Args:
            source_data: Dictionary containing source DataFrames
            
        Returns:
            Joined DataFrame
        """
        self.logger.info("Joining FAGLFLEXA and BSEG tables")
        
        try:
            faglflexa_df = source_data["FAGLFLEXA"]
            bseg_df = source_data["BSEG"]
            
            # Join FAGLFLEXA and BSEG on DOCNR = BELNR, RBUKRS = BUKRS, and RYEAR = GJAHR
            joined_df = faglflexa_df.join(
                bseg_df,
                (faglflexa_df.DOCNR == bseg_df.BELNR) &
                (faglflexa_df.RBUKRS == bseg_df.BUKRS) &
                (faglflexa_df.RYEAR == bseg_df.GJAHR),
                "left"
            )
            
            self.logger.info(f"Successfully joined tables. Joined rows: {joined_df.count()}")
            return joined_df
            
        except Exception as e:
            self.logger.error(f"Error joining source tables: {str(e)}")
            self._send_error_notification(f"Error joining source tables: {str(e)}")
            raise
    
    def _transform_data(self, joined_df: DataFrame, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Apply transformation logic to joined data
        
        Args:
            joined_df: Joined DataFrame from FAGLFLEXA and BSEG
            source_data: Dictionary containing source DataFrames
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Applying transformation logic")
        
        try:
            entity_view = source_data["ENTITY_VIEW"]
            gl_view = source_data["GL_VIEW"]
            trading_partner_view = source_data["TRADING_PARTNER_VIEW"]
            exchange_rate_df = source_data["EXCHANGE_RATE"]
            revenue_entity_df = source_data["REVENUE_ENTITY"]
            
            # Filter company codes that don't start with '8'
            filtered_df = joined_df.filter(~F.col("RBUKRS").startswith("8"))
            
            # Get local currency for each entity from revenue entity view
            local_currency_mapping = revenue_entity_df.select(
                F.col("entity_code").alias("entity"),
                F.col("local_currency")
            )
            
            # Transform data according to requirements
            transformed_df = filtered_df \
                .withColumn("FiscalYear", F.lit(self.fiscal_year)) \
                .withColumn("PostingPeriod", F.lit(self.posting_period)) \
                .withColumn("DocumentNumber", F.col("DOCNR")) \
                .withColumn("CompCode", F.col("RBUKRS"))
            
            # Join with entity view to get legal entity
            transformed_df = transformed_df.join(
                entity_view.select(
                    F.col("company_code").alias("comp_code"),
                    F.col("golden_entity").alias("legal_entity")
                ),
                transformed_df.CompCode == F.col("comp_code"),
                "left"
            ).drop("comp_code")
            
            # Set GLAccount and join with GL view to get golden GL account
            transformed_df = transformed_df \
                .withColumn("GLAccount", F.col("RACCT")) \
                .join(
                    gl_view.select(
                        F.col("gl_account").alias("gl_acct"),
                        F.col("golden_gl").alias("golden_gl_acct")
                    ),
                    transformed_df.GLAccount == F.col("gl_acct"),
                    "left"
                ).drop("gl_acct")
            
            # Set TradingPartner and join with trading partner view
            transformed_df = transformed_df \
                .withColumn("TradingPartner", F.col("RASSC")) \
                .join(
                    trading_partner_view.select(
                        F.col("trading_partner").alias("tp"),
                        F.col("golden_trading_partner").alias("golden_tp")
                    ),
                    transformed_df.TradingPartner == F.col("tp"),
                    "left"
                ).drop("tp")
            
            # Join with local currency mapping
            transformed_df = transformed_df.join(
                local_currency_mapping,
                transformed_df.legal_entity == local_currency_mapping.entity,
                "left"
            ).drop("entity")
            
            # Calculate GainLossLC
            transformed_df = transformed_df.withColumn(
                "GainLossLC",
                F.when(
                    F.col("local_currency").isin(LOCAL_CURRENCIES),
                    F.col("HSL") * 100
                ).otherwise(0)
            )
            
            # Set LocalCurrency
            transformed_df = transformed_df.withColumnRenamed("local_currency", "LocalCurrency")
            
            # Calculate GainLossTC
            transformed_df = transformed_df \
                .withColumn("TransactionCurrency", F.col("RWCUR")) \
                .withColumn(
                    "GainLossTC",
                    F.when(
                        F.col("TransactionCurrency").isin(TRANSACTIONAL_CURRENCIES),
                        F.col("TSL") * 100
                    ).otherwise(0)
                )
            
            # Calculate GainLossGC using exchange rates
            # For simplicity, we'll assume a join with exchange_rate_df would provide the conversion rate
            # In a real scenario, this would involve more complex logic with date-based rates
            transformed_df = transformed_df.withColumn("GainLossGC", F.lit(0))  # Placeholder
            
            # Apply logic for OffsetAccount
            # This is a simplified version of what would be more complex logic in reality
            transformed_df = transformed_df.withColumn(
                "OffsetAccount",
                F.when(
                    F.col("GLAccount").rlike("^[4-5]"),  # Realized accounts
                    F.col("HKONT")
                ).when(
                    F.col("GLAccount").rlike("^[6-7]"),  # Unrealized accounts
                    F.col("HKONT")
                ).otherwise(None)
            )
            
            # Join with GL view again to get golden offset account
            transformed_df = transformed_df.join(
                gl_view.select(
                    F.col("gl_account").alias("offset_gl"),
                    F.col("golden_gl").alias("golden_offset_gl")
                ),
                transformed_df.OffsetAccount == F.col("offset_gl"),
                "left"
            ).drop("offset_gl")
            
            # Set offset account amounts and clearing document number
            transformed_df = transformed_df \
                .withColumn("OffsetAccountLCAmount", F.col("DMBTR")) \
                .withColumn("OffsetAccountTCAmount", F.col("WRBTR")) \
                .withColumn("OffsetClearingDocumentNumber", F.col("AUGBL"))
            
            # Final column selection and renaming
            result_df = transformed_df.select(
                "FiscalYear",
                "PostingPeriod",
                "DocumentNumber",
                "CompCode",
                F.col("legal_entity").alias("LegalEntity"),
                "GLAccount",
                F.col("golden_gl_acct").alias("GoldenGLAcct"),
                "TradingPartner",
                F.col("golden_tp").alias("GoldenTradingPartner"),
                "GainLossGC",
                "GainLossLC",
                "LocalCurrency",
                "GainLossTC",
                "TransactionCurrency",
                "OffsetAccount",
                F.col("golden_offset_gl").alias("GoldenOffsetAccount"),
                "OffsetAccountLCAmount",
                "OffsetAccountTCAmount",
                "OffsetClearingDocumentNumber",
                F.lit("ECC Everest").alias("SourceSystem")
            )
            
            self.logger.info(f"Successfully transformed data. Result rows: {result_df.count()}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error transforming data: {str(e)}")
            self._send_error_notification(f"Error transforming data: {str(e)}")
            raise
    
    def _write_target_data(self, transformed_df: DataFrame) -> None:
        """
        Write transformed data to target table
        
        Args:
            transformed_df: Transformed DataFrame
        """
        self.logger.info(f"Writing data to target table: {TARGET_TABLE}")
        
        try:
            # Write data to target table
            transformed_df.write \
                .format("parquet") \
                .mode("append") \
                .saveAsTable(TARGET_TABLE)
            
            self.logger.info(f"Successfully wrote {transformed_df.count()} rows to {TARGET_TABLE}")
            
        except Exception as e:
            self.logger.error(f"Error writing to target table: {str(e)}")
            self._send_error_notification(f"Error writing to target table: {str(e)}")
            raise
    
    def _send_error_notification(self, error_message: str) -> None:
        """
        Send error notification email
        
        Args:
            error_message: Error message to send
        """
        # In a real implementation, this would send an email
        # For this example, we'll just log it
        self.logger.error(f"Error notification would be sent: {error_message}")
        self.logger.error(f"Recipients: {', '.join(EMAIL_CONFIG['recipients'])}")
    
    def process(self) -> None:
        """
        Main processing method to orchestrate the data flow
        """
        self.logger.info(f"Starting finance data processing for FY: {self.fiscal_year}, Period: {self.posting_period}")
        
        try:
            # Read source data
            source_data = self._read_source_data()
            
            # Join source tables
            joined_df = self._join_source_tables(source_data)
            
            # Transform data
            transformed_df = self._transform_data(joined_df, source_data)
            
            # Write to target
            self._write_target_data(transformed_df)
            
            self.logger.info("Finance data processing completed successfully")
            
        except Exception as e:
            self.logger.error(f"Finance data processing failed: {str(e)}")
            self._send_error_notification(f"Finance data processing failed: {str(e)}")
            raise


def main(fiscal_year: str, posting_period: str) -> None:
    """
    Main entry point for the finance data processor
    
    Args:
        fiscal_year: Fiscal year to process
        posting_period: Posting period to process
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"Finance Data Processor - FY{fiscal_year} P{posting_period}") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Process data
    processor = FinanceDataProcessor(spark, fiscal_year, posting_period)
    processor.process()


# Entry point for Databricks notebook
if __name__ == "__main__":
    # Get parameters from Databricks notebook
    dbutils_available = 'dbutils' in locals()
    
    if dbutils_available:
        # Running in Databricks notebook
        fiscal_year = dbutils.widgets.get("fiscal_year")
        posting_period = dbutils.widgets.get("posting_period")
    else:
        # Running as a script
        if len(sys.argv) < 3:
            print("Usage: python finance_processor.py <fiscal_year> <posting_period>")
            sys.exit(1)
        fiscal_year = sys.argv[1]
        posting_period = sys.argv[2]
    
    main(fiscal_year, posting_period)