"""
Finance Data Processor

This module processes financial data from FAGLFLEXA and BSEG tables, applies
transformations, and loads the results into the Finance table.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Dict, List, Optional, Tuple
import logging
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinanceDataProcessor:
    """
    Processes financial data from FAGLFLEXA and BSEG tables and loads it into the Finance table.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, str]):
        """
        Initialize the Finance Data Processor.
        
        Args:
            spark: SparkSession object
            config: Configuration dictionary containing source and target paths
        """
        self.spark = spark
        self.config = config
        
    def read_source_data(self) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Read source data from FAGLFLEXA, BSEG tables and Golden views.
        
        Returns:
            Tuple of DataFrames (faglflexa_df, bseg_df, entity_df, gl_df, trading_partner_df, exchange_rate_df)
        """
        logger.info("Reading source data")
        
        try:
            # Read FAGLFLEXA table
            faglflexa_df = self.spark.read.parquet(self.config["faglflexa_path"])
            logger.info(f"Successfully read FAGLFLEXA data with {faglflexa_df.count()} records")
            
            # Read BSEG table
            bseg_df = self.spark.read.parquet(self.config["bseg_path"])
            logger.info(f"Successfully read BSEG data with {bseg_df.count()} records")
            
            # Read Golden Entity view
            entity_df = self.spark.read.parquet(self.config["entity_view_path"])
            logger.info(f"Successfully read Entity view with {entity_df.count()} records")
            
            # Read Golden GL view
            gl_df = self.spark.read.parquet(self.config["gl_view_path"])
            logger.info(f"Successfully read GL view with {gl_df.count()} records")
            
            # Read Golden Trading Partner view
            trading_partner_df = self.spark.read.parquet(self.config["trading_partner_view_path"])
            logger.info(f"Successfully read Trading Partner view with {trading_partner_df.count()} records")
            
            # Read exchange rate data
            exchange_rate_df = self.spark.read.parquet(self.config["exchange_rate_path"])
            logger.info(f"Successfully read Exchange Rate data with {exchange_rate_df.count()} records")
            
            return faglflexa_df, bseg_df, entity_df, gl_df, trading_partner_df, exchange_rate_df
            
        except Exception as e:
            logger.error(f"Error reading source data: {str(e)}")
            self._log_error("read_source_data", str(e))
            raise
    
    def process_finance_data(self) -> None:
        """
        Main method to process finance data and write to target.
        """
        try:
            logger.info("Starting finance data processing")
            
            # Read source data
            faglflexa_df, bseg_df, entity_df, gl_df, trading_partner_df, exchange_rate_df = self.read_source_data()
            
            # Step 1: Join FAGLFLEXA with BSEG
            logger.info("Step 1: Joining FAGLFLEXA with BSEG")
            joined_df = self._join_faglflexa_bseg(faglflexa_df, bseg_df)
            
            # Step 2: Filter records
            logger.info("Step 2: Filtering records")
            filtered_df = self._filter_records(joined_df, entity_df)
            
            # Step 3-5: Convert to Golden values
            logger.info("Step 3-5: Converting to Golden values")
            golden_df = self._convert_to_golden_values(filtered_df, entity_df, gl_df, trading_partner_df)
            
            # Step 6-7: Calculate Gain/Loss
            logger.info("Step 6-7: Calculating Gain/Loss")
            gain_loss_df = self._calculate_gain_loss(golden_df, exchange_rate_df)
            
            # Step 8-9: Determine Offset Account
            logger.info("Step 8-9: Determining Offset Account")
            final_df = self._determine_offset_account(gain_loss_df, gl_df)
            
            # Write to target
            self._write_to_target(final_df)
            
            logger.info("Finance data processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error in finance data processing: {str(e)}")
            self._log_error("process_finance_data", str(e))
            raise
    
    def _join_faglflexa_bseg(self, faglflexa_df: DataFrame, bseg_df: DataFrame) -> DataFrame:
        """
        Join FAGLFLEXA with BSEG and apply initial filters.
        
        Args:
            faglflexa_df: FAGLFLEXA DataFrame
            bseg_df: BSEG DataFrame
            
        Returns:
            Joined DataFrame
        """
        try:
            # Filter FAGLFLEXA for RLDNR = '0L' and XBILK = 'X'
            faglflexa_filtered = faglflexa_df.filter(
                (F.col("RLDNR") == "0L") & (F.col("XBILK") == "X")
            )
            
            # Join with BSEG
            joined_df = faglflexa_filtered.join(
                bseg_df,
                (faglflexa_filtered.DOCNR == bseg_df.BELNR) &
                (faglflexa_filtered.RBUKRS == bseg_df.BUKRS) &
                (faglflexa_filtered.RYEAR == bseg_df.GJAHR),
                "left"
            )
            
            return joined_df
        
        except Exception as e:
            logger.error(f"Error joining FAGLFLEXA with BSEG: {str(e)}")
            self._log_error("_join_faglflexa_bseg", str(e))
            raise
    
    def _filter_records(self, joined_df: DataFrame, entity_df: DataFrame) -> DataFrame:
        """
        Filter out records based on CompCode and HistEntity.
        
        Args:
            joined_df: Joined DataFrame
            entity_df: Entity DataFrame
            
        Returns:
            Filtered DataFrame
        """
        try:
            # Filter out records with CompCode starting with '8%'
            filtered_df = joined_df.filter(~F.col("RBUKRS").like("8%"))
            
            # Join with Entity view to filter out archived entities
            filtered_df = filtered_df.join(
                entity_df.select("CompanyCode", "HistEntity"),
                filtered_df.RBUKRS == entity_df.CompanyCode,
                "left"
            ).filter(
                (F.col("HistEntity").isNull()) | (F.col("HistEntity") != "Y")
            ).drop("HistEntity")
            
            return filtered_df
        
        except Exception as e:
            logger.error(f"Error filtering records: {str(e)}")
            self._log_error("_filter_records", str(e))
            raise
    
    def _convert_to_golden_values(self, filtered_df: DataFrame, 
                                 entity_df: DataFrame, 
                                 gl_df: DataFrame,
                                 trading_partner_df: DataFrame) -> DataFrame:
        """
        Convert source values to Golden values using mapping views.
        
        Args:
            filtered_df: Filtered DataFrame
            entity_df: Entity mapping DataFrame
            gl_df: GL account mapping DataFrame
            trading_partner_df: Trading Partner mapping DataFrame
            
        Returns:
            DataFrame with Golden values
        """
        try:
            # Step 3: Convert Company Code to Golden Entity
            golden_df = filtered_df.join(
                entity_df.select("CompanyCode", "Entity"),
                filtered_df.RBUKRS == entity_df.CompanyCode,
                "left"
            ).withColumnRenamed("Entity", "GoldenEntity")
            
            # Step 4: Convert RACCT to Golden GL account
            golden_df = golden_df.join(
                gl_df.select("SourceGLAccount", "GoldenGLAccount"),
                golden_df.RACCT == gl_df.SourceGLAccount,
                "left"
            ).withColumnRenamed("GoldenGLAccount", "GoldenGL")
            
            # Step 5: Convert RASSC to Golden Trading Partner
            golden_df = golden_df.join(
                trading_partner_df.select("SourceTradingPartner", "GoldenTradingPartner"),
                golden_df.RASSC == trading_partner_df.SourceTradingPartner,
                "left"
            ).withColumnRenamed("GoldenTradingPartner", "GoldenTradingPartner")
            
            return golden_df
        
        except Exception as e:
            logger.error(f"Error converting to Golden values: {str(e)}")
            self._log_error("_convert_to_golden_values", str(e))
            raise
    
    def _calculate_gain_loss(self, golden_df: DataFrame, exchange_rate_df: DataFrame) -> DataFrame:
        """
        Calculate Gain/Loss in Group Currency, Local Currency, and Transactional Currency.
        
        Args:
            golden_df: DataFrame with Golden values
            exchange_rate_df: Exchange rate DataFrame
            
        Returns:
            DataFrame with calculated Gain/Loss
        """
        try:
            # Step 6: Calculate GainLossGC using BPC exchange rates
            # Join with exchange rate data
            gain_loss_df = golden_df.join(
                exchange_rate_df,
                (golden_df.RBUKRS == exchange_rate_df.CompanyCode) &
                (golden_df.RYEAR == exchange_rate_df.FiscalYear) &
                (golden_df.POPER == exchange_rate_df.Period),
                "left"
            )
            
            # Calculate GainLossGC
            gain_loss_df = gain_loss_df.withColumn(
                "GainLossGC",
                F.when(F.col("ExchangeRate").isNotNull(), 
                       F.col("HSL") * F.col("ExchangeRate")).otherwise(F.lit(0))
            )
            
            # Step 7: Calculate GainLossLC and GainLossTC
            gain_loss_df = gain_loss_df.withColumn(
                "GainLossLC",
                F.when(F.col("LocalCurrency").isNotNull(), 
                       F.col("HSL") * F.col("LocalCurrencyRate")).otherwise(F.lit(0))
            ).withColumn(
                "GainLossTC",
                F.when(F.col("TransactionalCurrency").isNotNull(), 
                       F.col("HSL") * F.col("TransactionalCurrencyRate")).otherwise(F.lit(0))
            )
            
            return gain_loss_df
        
        except Exception as e:
            logger.error(f"Error calculating Gain/Loss: {str(e)}")
            self._log_error("_calculate_gain_loss", str(e))
            raise
    
    def _determine_offset_account(self, gain_loss_df: DataFrame, gl_df: DataFrame) -> DataFrame:
        """
        Determine OffsetAccount for Realized and Unrealized accounts and convert to Golden GL.
        
        Args:
            gain_loss_df: DataFrame with calculated Gain/Loss
            gl_df: GL account mapping DataFrame
            
        Returns:
            Final DataFrame with OffsetAccount
        """
        try:
            # Step 8: Determine OffsetAccount based on specified logic
            offset_df = gain_loss_df.withColumn(
                "OffsetAccount",
                F.when(F.col("RACCT").isin(self.config["realized_accounts"]), 
                       F.lit(self.config["realized_offset_account"]))
                .when(F.col("RACCT").isin(self.config["unrealized_accounts"]), 
                      F.lit(self.config["unrealized_offset_account"]))
                .otherwise(F.lit(None))
            )
            
            # Step 9: Convert OffsetAccount to Golden GL account
            final_df = offset_df.join(
                gl_df.select("SourceGLAccount", "GoldenGLAccount").alias("offset_gl"),
                offset_df.OffsetAccount == F.col("offset_gl.SourceGLAccount"),
                "left"
            ).withColumn(
                "GoldenOffsetGL", 
                F.col("offset_gl.GoldenGLAccount")
            ).drop("offset_gl.SourceGLAccount", "offset_gl.GoldenGLAccount")
            
            # Select and rename final columns
            final_columns = [
                "DOCNR", "RBUKRS", "RYEAR", "POPER", "RACCT", "RASSC", 
                "HSL", "GoldenEntity", "GoldenGL", "GoldenTradingPartner",
                "GainLossGC", "GainLossLC", "GainLossTC", "OffsetAccount", "GoldenOffsetGL",
                F.current_timestamp().alias("ProcessedTimestamp")
            ]
            
            final_df = final_df.select(*final_columns)
            
            return final_df
        
        except Exception as e:
            logger.error(f"Error determining OffsetAccount: {str(e)}")
            self._log_error("_determine_offset_account", str(e))
            raise
    
    def _write_to_target(self, final_df: DataFrame) -> None:
        """
        Write the final DataFrame to the target Finance table.
        
        Args:
            final_df: Final processed DataFrame
        """
        try:
            logger.info(f"Writing {final_df.count()} records to Finance table")
            
            # Write to Delta table
            final_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(self.config["finance_table_path"])
            
            logger.info("Successfully wrote data to Finance table")
            
        except Exception as e:
            logger.error(f"Error writing to target: {str(e)}")
            self._log_error("_write_to_target", str(e))
            raise
    
    def _log_error(self, function_name: str, error_message: str) -> None:
        """
        Log error to error log table.
        
        Args:
            function_name: Name of the function where error occurred
            error_message: Error message
        """
        try:
            error_data = [
                (
                    function_name,
                    error_message,
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
            ]
            
            error_df = self.spark.createDataFrame(
                error_data,
                ["FunctionName", "ErrorMessage", "ErrorTimestamp"]
            )
            
            error_df.write \
                .format("delta") \
                .mode("append") \
                .save(self.config["error_log_path"])
                
        except Exception as e:
            logger.error(f"Failed to log error to error log table: {str(e)}")


def get_config() -> Dict[str, str]:
    """
    Get configuration for the Finance Data Processor.
    
    Returns:
        Configuration dictionary
    """
    return {
        # Source paths
        "faglflexa_path": "/mnt/everest_ecc/FAGLFLEXA",
        "bseg_path": "/mnt/everest_ecc/BSEG",
        "entity_view_path": "/mnt/golden_views/Entity",
        "gl_view_path": "/mnt/golden_views/GL",
        "trading_partner_view_path": "/mnt/golden_views/TradingPartner",
        "exchange_rate_path": "/mnt/bpc/s_shared/v_actual_exchange_rate_bpc",
        
        # Target path
        "finance_table_path": "/mnt/finance/Finance",
        
        # Error log path
        "error_log_path": "/mnt/logs/finance_processor_errors",
        
        # Business logic configuration
        "realized_accounts": ["40001", "40002", "40003"],
        "unrealized_accounts": ["50001", "50002", "50003"],
        "realized_offset_account": "45000",
        "unrealized_offset_account": "55000"
    }


def create_spark_session() -> SparkSession:
    """
    Create a SparkSession for the Finance Data Processor.
    
    Returns:
        SparkSession object
    """
    return SparkSession.builder \
        .appName("Finance Data Processor") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()


def main():
    """
    Main entry point for the Finance Data Processor.
    """
    try:
        logger.info("Starting Finance Data Processor")
        
        # Create SparkSession
        spark = create_spark_session()
        
        # Get configuration
        config = get_config()
        
        # Create and run processor
        processor = FinanceDataProcessor(spark, config)
        processor.process_finance_data()
        
        logger.info("Finance Data Processor completed successfully")
        
    except Exception as e:
        logger.error(f"Finance Data Processor failed: {str(e)}")
        raise
    finally:
        logger.info("Finance Data Processor finished")


if __name__ == "__main__":
    main()