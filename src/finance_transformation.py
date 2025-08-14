"""
Finance data transformation module for processing FAGLFLEXA and BSEG data.
This module implements the technical requirement TR-FIN-001.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinanceTransformation:
    """
    Class to handle finance data transformation from FAGLFLEXA and BSEG tables.
    """
    
    def __init__(self, spark, config):
        """
        Initialize the transformation class with SparkSession and configuration.
        
        Args:
            spark (SparkSession): Active Spark session
            config (dict): Configuration parameters including paths and fiscal parameters
        """
        self.spark = spark
        self.config = config
        self.logger = logger

    def read_source_data(self):
        """
        Read all required source data tables.
        
        Returns:
            tuple: Dataframes for FAGLFLEXA, BSEG, and reference tables
        """
        try:
            self.logger.info("Reading source data tables")
            
            # Read FAGLFLEXA table
            faglflexa_df = self.spark.read.parquet(self.config["faglflexa_path"])
            self.logger.info(f"FAGLFLEXA record count: {faglflexa_df.count()}")
            
            # Read BSEG table
            bseg_df = self.spark.read.parquet(self.config["bseg_path"])
            self.logger.info(f"BSEG record count: {bseg_df.count()}")
            
            # Read Golden Entity view
            golden_entity_df = self.spark.read.parquet(self.config["golden_entity_path"])
            self.logger.info(f"Golden Entity record count: {golden_entity_df.count()}")
            
            # Read Golden GL view
            golden_gl_df = self.spark.read.parquet(self.config["golden_gl_path"])
            self.logger.info(f"Golden GL record count: {golden_gl_df.count()}")
            
            # Read Golden Trading Partner view
            golden_tp_df = self.spark.read.parquet(self.config["golden_trading_partner_path"])
            self.logger.info(f"Golden Trading Partner record count: {golden_tp_df.count()}")
            
            # Read Exchange Rate table
            exchange_rate_df = self.spark.read.parquet(self.config["exchange_rate_path"])
            self.logger.info(f"Exchange Rate record count: {exchange_rate_df.count()}")
            
            return faglflexa_df, bseg_df, golden_entity_df, golden_gl_df, golden_tp_df, exchange_rate_df
            
        except Exception as e:
            self.logger.error(f"Error reading source data: {str(e)}")
            raise

    def filter_by_fiscal_period(self, faglflexa_df):
        """
        Filter FAGLFLEXA data based on fiscal year and posting period.
        
        Args:
            faglflexa_df (DataFrame): FAGLFLEXA data
            
        Returns:
            DataFrame: Filtered FAGLFLEXA data
        """
        try:
            self.logger.info(f"Filtering data for FY: {self.config['fiscal_year']} and Period: {self.config['posting_period']}")
            
            filtered_df = faglflexa_df.filter(
                (F.col("RYEAR") == self.config["fiscal_year"]) & 
                (F.col("POPER") == self.config["posting_period"])
            )
            
            self.logger.info(f"Records after fiscal filtering: {filtered_df.count()}")
            return filtered_df
        
        except Exception as e:
            self.logger.error(f"Error filtering by fiscal period: {str(e)}")
            raise

    def join_and_transform_data(self, faglflexa_df, bseg_df, golden_entity_df, golden_gl_df, golden_tp_df, exchange_rate_df):
        """
        Join and transform the data according to business requirements.
        
        Args:
            faglflexa_df (DataFrame): FAGLFLEXA data
            bseg_df (DataFrame): BSEG data
            golden_entity_df (DataFrame): Golden Entity data
            golden_gl_df (DataFrame): Golden GL data
            golden_tp_df (DataFrame): Golden Trading Partner data
            exchange_rate_df (DataFrame): Exchange Rate data
            
        Returns:
            DataFrame: Transformed finance data
        """
        try:
            self.logger.info("Starting data join and transformation")
            
            # Join FAGLFLEXA with BSEG
            joined_df = faglflexa_df.join(
                bseg_df,
                (faglflexa_df.DOCNR == bseg_df.BELNR) &
                (faglflexa_df.RBUKRS == bseg_df.BUKRS) &
                (faglflexa_df.RYEAR == bseg_df.GJAHR),
                "left"
            )
            
            self.logger.info(f"Records after joining FAGLFLEXA and BSEG: {joined_df.count()}")
            
            # Derive fiscal fields
            transformed_df = joined_df.withColumn(
                "FiscalYear", F.col("RYEAR")
            ).withColumn(
                "PostingPeriod", F.col("POPER")
            ).withColumn(
                "SourceFiscalYear", F.col("RYEAR")
            ).withColumn(
                "SourcePeriod", F.col("POPER")
            )
            
            # Extract document information
            transformed_df = transformed_df.withColumn(
                "DocumentNumber", F.col("DOCNR")
            ).withColumn(
                "CompCode", F.col("RBUKRS")
            )
            
            # Join with Golden Entity for Legal Entity mapping
            transformed_df = transformed_df.join(
                golden_entity_df,
                transformed_df.CompCode == golden_entity_df.SourceCompanyCode,
                "left"
            ).withColumn(
                "LegalEntity", F.col("GoldenEntityID")
            )
            
            # Filter out records with archived Golden Entity values
            archived_count = transformed_df.filter(F.col("IsArchived") == True).count()
            if archived_count > 0:
                self.logger.warning(f"Excluding {archived_count} records with archived Golden Entity values")
            
            transformed_df = transformed_df.filter(
                (F.col("IsArchived").isNull()) | (F.col("IsArchived") == False)
            )
            
            # GL Account mapping
            transformed_df = transformed_df.withColumn(
                "GLAccount", F.col("RACCT")
            )
            
            # Join with Golden GL for GL Account mapping
            transformed_df = transformed_df.join(
                golden_gl_df,
                transformed_df.GLAccount == golden_gl_df.SourceGLAccount,
                "left"
            ).withColumn(
                "GoldenGLAcct", F.col("GoldenGLID")
            )
            
            # Trading Partner derivation
            transformed_df = transformed_df.withColumn(
                "TradingPartner", F.col("PRCTR")
            )
            
            # Join with Golden Trading Partner
            transformed_df = transformed_df.join(
                golden_tp_df,
                transformed_df.TradingPartner == golden_tp_df.SourceTradingPartner,
                "left"
            ).withColumn(
                "GoldenTradingPartner", F.col("GoldenTradingPartnerID")
            )
            
            # Currency and exchange rate calculations
            transformed_df = transformed_df.withColumn(
                "LocalCurrency", F.col("RHCUR")
            ).withColumn(
                "TransactionCurrency", F.col("RKCUR")
            )
            
            # Join with exchange rates for gain/loss calculations
            transformed_df = transformed_df.join(
                exchange_rate_df,
                (transformed_df.LocalCurrency == exchange_rate_df.FromCurrency) &
                (F.lit("USD") == exchange_rate_df.ToCurrency) &
                (transformed_df.FiscalYear == exchange_rate_df.FiscalYear) &
                (transformed_df.PostingPeriod == exchange_rate_df.Period),
                "left"
            )
            
            # Calculate gain/loss amounts
            transformed_df = transformed_df.withColumn(
                "GainLossLC", 
                F.when(F.col("LocalCurrency") == F.col("TransactionCurrency"), 0)
                 .otherwise(F.col("HSL") - F.col("KSL"))
            ).withColumn(
                "GainLossGC",
                F.when(F.col("LocalCurrency") == "USD", F.col("GainLossLC"))
                 .otherwise(F.col("GainLossLC") * F.col("ExchangeRate"))
            ).withColumn(
                "GainLossTC",
                F.when(F.col("TransactionCurrency") == "USD", F.col("GainLossLC") * F.col("ExchangeRate"))
                 .otherwise(0)
            )
            
            # Determine if realized or unrealized gain/loss
            transformed_df = transformed_df.withColumn(
                "IsRealized", 
                F.when(F.col("BLART").isin(["AB", "DZ", "KZ"]), True).otherwise(False)
            )
            
            # Derive offset account based on realized/unrealized logic
            transformed_df = transformed_df.withColumn(
                "OffsetAccount",
                F.when(
                    F.col("IsRealized"), 
                    F.when(F.col("GainLossGC") >= 0, self.config["realized_gain_account"])
                     .otherwise(self.config["realized_loss_account"])
                ).otherwise(
                    F.when(F.col("GainLossGC") >= 0, self.config["unrealized_gain_account"])
                     .otherwise(self.config["unrealized_loss_account"])
                )
            )
            
            # Join with Golden GL for Offset Account mapping
            offset_gl_df = golden_gl_df.withColumnRenamed("SourceGLAccount", "OffsetSourceGL")
            offset_gl_df = offset_gl_df.withColumnRenamed("GoldenGLID", "GoldenOffsetGLID")
            
            transformed_df = transformed_df.join(
                offset_gl_df,
                transformed_df.OffsetAccount == offset_gl_df.OffsetSourceGL,
                "left"
            ).withColumn(
                "GoldenOffsetAccount", F.col("GoldenOffsetGLID")
            )
            
            # Select final columns for output
            final_df = transformed_df.select(
                "FiscalYear",
                "PostingPeriod",
                "SourceFiscalYear",
                "SourcePeriod",
                "DocumentNumber",
                "CompCode",
                "LegalEntity",
                "GLAccount",
                "GoldenGLAcct",
                "TradingPartner",
                "GoldenTradingPartner",
                "LocalCurrency",
                "TransactionCurrency",
                "GainLossLC",
                "GainLossGC",
                "GainLossTC",
                "IsRealized",
                "OffsetAccount",
                "GoldenOffsetAccount",
                F.current_timestamp().alias("ProcessedTimestamp")
            )
            
            self.logger.info(f"Final transformed record count: {final_df.count()}")
            return final_df
            
        except Exception as e:
            self.logger.error(f"Error in join and transform: {str(e)}")
            raise

    def write_finance_table(self, finance_df):
        """
        Write the transformed data to the Finance table.
        
        Args:
            finance_df (DataFrame): Transformed finance data
        """
        try:
            self.logger.info(f"Writing {finance_df.count()} records to Finance table")
            
            # Write to target location
            finance_df.write.mode("overwrite").parquet(self.config["finance_table_path"])
            
            self.logger.info("Successfully wrote data to Finance table")
            
        except Exception as e:
            self.logger.error(f"Error writing to Finance table: {str(e)}")
            raise

    def run_transformation(self):
        """
        Execute the full transformation pipeline.
        """
        try:
            self.logger.info("Starting finance transformation pipeline")
            
            # Read source data
            faglflexa_df, bseg_df, golden_entity_df, golden_gl_df, golden_tp_df, exchange_rate_df = self.read_source_data()
            
            # Filter by fiscal period
            filtered_faglflexa_df = self.filter_by_fiscal_period(faglflexa_df)
            
            # Join and transform data
            transformed_df = self.join_and_transform_data(
                filtered_faglflexa_df, 
                bseg_df, 
                golden_entity_df, 
                golden_gl_df, 
                golden_tp_df, 
                exchange_rate_df
            )
            
            # Write to finance table
            self.write_finance_table(transformed_df)
            
            self.logger.info("Finance transformation pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Finance transformation pipeline failed: {str(e)}")
            raise


def run_finance_job(spark, params):
    """
    Main entry point for the finance transformation job.
    
    Args:
        spark (SparkSession): Active Spark session
        params (dict): Job parameters
    """
    logger.info(f"Starting finance job with parameters: {params}")
    
    # Default configuration
    config = {
        "faglflexa_path": params.get("faglflexa_path", "/mnt/everest-ecc/FAGLFLEXA"),
        "bseg_path": params.get("bseg_path", "/mnt/everest-ecc/BSEG"),
        "golden_entity_path": params.get("golden_entity_path", "/mnt/golden-views/entity"),
        "golden_gl_path": params.get("golden_gl_path", "/mnt/golden-views/gl"),
        "golden_trading_partner_path": params.get("golden_trading_partner_path", "/mnt/golden-views/trading_partner"),
        "exchange_rate_path": params.get("exchange_rate_path", "/mnt/bpc/s_shared/v_actual_exchange_rate_bpc"),
        "finance_table_path": params.get("finance_table_path", "/mnt/finance/finance_table"),
        "fiscal_year": params.get("fiscal_year", datetime.now().year),
        "posting_period": params.get("posting_period", datetime.now().month),
        "realized_gain_account": params.get("realized_gain_account", "425000"),
        "realized_loss_account": params.get("realized_loss_account", "525000"),
        "unrealized_gain_account": params.get("unrealized_gain_account", "426000"),
        "unrealized_loss_account": params.get("unrealized_loss_account", "526000")
    }
    
    # Create and run the transformation
    finance_transformation = FinanceTransformation(spark, config)
    finance_transformation.run_transformation()
    
    logger.info("Finance job completed successfully")


if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Finance Data Transformation") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("INFO")
    
    # Run the job with default parameters
    # In production, parameters would be passed from job scheduler
    run_finance_job(spark, {})