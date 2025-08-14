"""
Finance Data Processor

This module processes financial data from ECC Everest source system and transforms it
according to the requirements specified in TR-FIN-001.
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, DateType
from pyspark.sql.window import Window
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("finance_data_processor")

class FinanceDataProcessor:
    """
    Processes financial data from ECC Everest source system and generates transformed output.
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize the Finance Data Processor.
        
        Args:
            spark: SparkSession object
            config: Configuration dictionary containing source and target paths
        """
        self.spark = spark
        self.config = config
        self.retry_count = 3
        self.admin_email = config.get("admin_email", "admin@company.com")
    
    def read_source_data(self) -> Dict[str, DataFrame]:
        """
        Read all required source data.
        
        Returns:
            Dictionary containing all source DataFrames
        """
        logger.info("Reading source data...")
        
        try:
            # Read SAP tables
            faglflexa_df = self._read_with_retry(
                self.config["faglflexa_path"], 
                "FAGLFLEXA table"
            )
            
            bseg_df = self._read_with_retry(
                self.config["bseg_path"], 
                "BSEG table"
            )
            
            # Read golden views
            entity_golden_view = self._read_with_retry(
                self.config["entity_golden_view_path"], 
                "Entity golden view"
            )
            
            gl_golden_view = self._read_with_retry(
                self.config["gl_golden_view_path"], 
                "GL golden view"
            )
            
            trading_partner_golden_view = self._read_with_retry(
                self.config["trading_partner_golden_view_path"], 
                "Trading Partner golden view"
            )
            
            # Read exchange rates
            exchange_rates = self._read_with_retry(
                self.config["exchange_rates_path"], 
                "Exchange rates"
            )
            
            return {
                "faglflexa": faglflexa_df,
                "bseg": bseg_df,
                "entity_golden_view": entity_golden_view,
                "gl_golden_view": gl_golden_view,
                "trading_partner_golden_view": trading_partner_golden_view,
                "exchange_rates": exchange_rates
            }
            
        except Exception as e:
            error_msg = f"Failed to read source data: {str(e)}"
            logger.error(error_msg)
            self._send_notification(error_msg)
            raise
    
    def _read_with_retry(self, path: str, source_name: str) -> DataFrame:
        """
        Read data source with retry logic.
        
        Args:
            path: Path to the data source
            source_name: Name of the data source for logging
            
        Returns:
            DataFrame containing the source data
        """
        attempts = 0
        last_exception = None
        
        while attempts < self.retry_count:
            try:
                logger.info(f"Reading {source_name} from {path}")
                return self.spark.read.parquet(path)
            except Exception as e:
                attempts += 1
                last_exception = e
                logger.warning(f"Attempt {attempts} failed to read {source_name}: {str(e)}")
                if attempts < self.retry_count:
                    logger.info(f"Retrying... ({attempts}/{self.retry_count})")
                
        error_msg = f"Failed to read {source_name} after {self.retry_count} attempts"
        logger.error(error_msg)
        self._send_notification(error_msg)
        raise last_exception
    
    def process_data(self, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Process the source data according to business requirements.
        
        Args:
            source_data: Dictionary containing all source DataFrames
            
        Returns:
            Processed DataFrame
        """
        logger.info("Processing financial data...")
        
        try:
            # Extract source DataFrames
            faglflexa_df = source_data["faglflexa"]
            bseg_df = source_data["bseg"]
            entity_golden_view = source_data["entity_golden_view"]
            gl_golden_view = source_data["gl_golden_view"]
            trading_partner_golden_view = source_data["trading_partner_golden_view"]
            exchange_rates = source_data["exchange_rates"]
            
            # Step 1: Join FAGLFLEXA with BSEG and apply initial filters
            logger.info("Joining FAGLFLEXA with BSEG and applying filters...")
            joined_df = faglflexa_df.join(
                bseg_df,
                on=["DOCNR", "RBUKRS", "RYEAR"],
                how="inner"
            ).filter(
                (F.col("RLDNR") == "0L") & 
                (F.col("XBILK") == "X") &
                (~F.col("RBUKRS").like("8%"))
            )
            
            # Step 2: Convert Company Code to Golden Entity
            logger.info("Converting Company Code to Golden Entity...")
            with_entity_df = joined_df.join(
                entity_golden_view,
                joined_df["RBUKRS"] == entity_golden_view["source_company_code"],
                "left"
            ).select(
                joined_df["*"],
                entity_golden_view["golden_entity_id"].alias("GoldenEntityId")
            )
            
            # Step 3: Convert FAGLFLEXA.RACCT to Golden GL account
            logger.info("Converting RACCT to Golden GL account...")
            with_gl_df = with_entity_df.join(
                gl_golden_view,
                with_entity_df["RACCT"] == gl_golden_view["source_gl_account"],
                "left"
            ).select(
                with_entity_df["*"],
                gl_golden_view["golden_gl_account_id"].alias("GoldenGLAccountId")
            )
            
            # Step 4: Convert FAGLFLEXA.RASSC to Golden Trading Partner
            logger.info("Converting RASSC to Golden Trading Partner...")
            with_tp_df = with_gl_df.join(
                trading_partner_golden_view,
                with_gl_df["RASSC"] == trading_partner_golden_view["source_trading_partner"],
                "left"
            ).select(
                with_gl_df["*"],
                trading_partner_golden_view["golden_trading_partner_id"].alias("GoldenTradingPartnerId")
            )
            
            # Step 5: Calculate GainLossGC and GainLossLC using exchange rates
            logger.info("Calculating GainLoss values...")
            with_exchange_rates = self._calculate_gain_loss(with_tp_df, exchange_rates)
            
            # Step 6: Determine offset account and convert to Golden GL account
            logger.info("Determining offset account...")
            result_df = self._determine_offset_account(with_exchange_rates, gl_golden_view)
            
            # Step 7: Select and rename final columns
            logger.info("Preparing final output...")
            final_df = self._prepare_final_output(result_df)
            
            return final_df
            
        except Exception as e:
            error_msg = f"Error during data processing: {str(e)}"
            logger.error(error_msg)
            self._send_notification(error_msg)
            raise
    
    def _calculate_gain_loss(self, df: DataFrame, exchange_rates: DataFrame) -> DataFrame:
        """
        Calculate GainLossGC and GainLossLC using exchange rates.
        
        Args:
            df: Input DataFrame
            exchange_rates: Exchange rates DataFrame
            
        Returns:
            DataFrame with calculated gain/loss columns
        """
        # Join with exchange rates
        with_rates_df = df.join(
            exchange_rates,
            (df["RBUKRS"] == exchange_rates["company_code"]) &
            (df["BLDAT"] == exchange_rates["rate_date"]),
            "left"
        )
        
        # Apply special logic for certain Local Currencies
        result_df = with_rates_df.withColumn(
            "ExchangeRate",
            F.when(
                F.col("local_currency").isin(["USD", "EUR", "GBP"]),
                F.col("special_rate")
            ).otherwise(
                F.col("standard_rate")
            )
        )
        
        # Calculate GainLossGC and GainLossLC
        result_df = result_df.withColumn(
            "GainLossGC",
            F.when(
                F.col("ExchangeRate").isNotNull(),
                (F.col("HSL") * F.col("ExchangeRate")) - F.col("KSL")
            ).otherwise(F.lit(0))
        ).withColumn(
            "GainLossLC",
            F.when(
                F.col("ExchangeRate").isNotNull(),
                F.col("HSL") - (F.col("KSL") / F.col("ExchangeRate"))
            ).otherwise(F.lit(0))
        )
        
        return result_df
    
    def _determine_offset_account(self, df: DataFrame, gl_golden_view: DataFrame) -> DataFrame:
        """
        Determine offset account and convert to Golden GL account.
        
        Args:
            df: Input DataFrame
            gl_golden_view: GL golden view DataFrame
            
        Returns:
            DataFrame with offset account information
        """
        # Determine offset account based on business rules
        with_offset_df = df.withColumn(
            "OffsetAccount",
            F.when(
                F.col("GainLossGC") > 0,
                F.lit("FX_GAIN_ACCOUNT")
            ).when(
                F.col("GainLossGC") < 0,
                F.lit("FX_LOSS_ACCOUNT")
            ).otherwise(
                F.lit(None)
            )
        )
        
        # Join with GL golden view to get Golden GL account for offset
        result_df = with_offset_df.join(
            gl_golden_view.select(
                F.col("source_gl_account").alias("OffsetSourceAccount"),
                F.col("golden_gl_account_id").alias("OffsetGoldenGLAccountId")
            ),
            with_offset_df["OffsetAccount"] == F.col("OffsetSourceAccount"),
            "left"
        )
        
        return result_df
    
    def _prepare_final_output(self, df: DataFrame) -> DataFrame:
        """
        Select and rename final columns for output.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Final output DataFrame
        """
        # Select and rename columns as per requirements
        final_df = df.select(
            F.col("DOCNR").alias("DocumentNumber"),
            F.col("RYEAR").alias("FiscalYear"),
            F.col("POPER").alias("Period"),
            F.col("BLDAT").alias("PostingDate"),
            F.col("BUDAT").alias("DocumentDate"),
            F.col("GoldenEntityId"),
            F.col("GoldenGLAccountId"),
            F.col("GoldenTradingPartnerId"),
            F.col("RWCUR").alias("TransactionCurrency"),
            F.col("HSL").alias("AmountInLocalCurrency"),
            F.col("KSL").alias("AmountInGroupCurrency"),
            F.col("GainLossLC"),
            F.col("GainLossGC"),
            F.col("OffsetGoldenGLAccountId")
        )
        
        # Handle null values
        final_df = final_df.na.fill({
            "GoldenTradingPartnerId": "UNKNOWN",
            "OffsetGoldenGLAccountId": "UNKNOWN"
        })
        
        return final_df
    
    def write_output(self, df: DataFrame) -> None:
        """
        Write the processed data to the target location.
        
        Args:
            df: DataFrame to write
        """
        logger.info(f"Writing output to {self.config['output_path']}...")
        
        try:
            # Add processing timestamp
            output_df = df.withColumn(
                "ProcessingTimestamp", 
                F.current_timestamp()
            )
            
            # Write as Parquet with partitioning
            output_df.write \
                .mode("overwrite") \
                .partitionBy("FiscalYear", "Period") \
                .parquet(self.config["output_path"])
                
            logger.info("Output written successfully")
            
        except Exception as e:
            error_msg = f"Failed to write output data: {str(e)}"
            logger.error(error_msg)
            self._send_notification(error_msg)
            raise
    
    def run(self) -> None:
        """
        Run the entire data processing pipeline.
        """
        logger.info("Starting finance data processing job...")
        
        try:
            # Read source data
            source_data = self.read_source_data()
            
            # Process data
            processed_data = self.process_data(source_data)
            
            # Write output
            self.write_output(processed_data)
            
            logger.info("Finance data processing job completed successfully")
            
        except Exception as e:
            error_msg = f"Finance data processing job failed: {str(e)}"
            logger.error(error_msg)
            self._send_notification(error_msg)
            raise
    
    def _send_notification(self, message: str) -> None:
        """
        Send notification email to administrators.
        
        Args:
            message: Notification message
        """
        logger.info(f"Sending notification to {self.admin_email}: {message}")
        # In a real implementation, this would send an email
        # For now, we'll just log the notification
        pass


def create_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession.
    
    Returns:
        Configured SparkSession
    """
    return SparkSession.builder \
        .appName("Finance Data Processor") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .getOrCreate()


def main():
    """
    Main entry point for the finance data processing job.
    """
    # Create SparkSession
    spark = create_spark_session()
    
    # Configuration
    config = {
        "faglflexa_path": "/mnt/data/ecc_everest/faglflexa",
        "bseg_path": "/mnt/data/ecc_everest/bseg",
        "entity_golden_view_path": "/mnt/data/golden_views/entity",
        "gl_golden_view_path": "/mnt/data/golden_views/gl",
        "trading_partner_golden_view_path": "/mnt/data/golden_views/trading_partner",
        "exchange_rates_path": "/mnt/data/bpc/s_shared.v_actual_exchange_rate_bpc",
        "output_path": "/mnt/data/finance/output",
        "admin_email": "finance.admin@company.com"
    }
    
    # Create and run processor
    processor = FinanceDataProcessor(spark, config)
    processor.run()


if __name__ == "__main__":
    main()