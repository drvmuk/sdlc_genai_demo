"""
Finance Data Processing Module

This module contains the PySpark job to extract data from ECC Everest,
apply transformations, and load the transformed data into the Finance table.

Technical Requirement ID: TR-FIN-001
Related Functional Requirement: FR-FIN-001
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("finance_data_processor")

# Constants
LOCAL_CURRENCY_MULT_FACTOR = ["JPY", "KRW", "IDR", "CLP", "COP", "VND"]
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30

class FinanceDataProcessor:
    """Main class for processing finance data from ECC Everest to Finance table."""
    
    def __init__(self, spark: SparkSession, fiscal_year: int, posting_period: int):
        """
        Initialize the Finance Data Processor.
        
        Args:
            spark: SparkSession to use for data processing
            fiscal_year: Fiscal year to process
            posting_period: Posting period to process
        """
        self.spark = spark
        self.fiscal_year = fiscal_year
        self.posting_period = posting_period
        logger.info(f"Initializing Finance Data Processor for FY{fiscal_year} P{posting_period}")
        
    def run(self) -> None:
        """Execute the finance data processing pipeline."""
        try:
            logger.info("Starting finance data processing job")
            
            # Stage 1: Retrieve data from ECC Everest
            faglflexa_df = self._extract_faglflexa_data()
            bseg_df = self._extract_bseg_data()
            entity_golden_view_df = self._extract_entity_golden_view()
            golden_gl_view_df = self._extract_golden_gl_view()
            golden_trading_partner_df = self._extract_golden_trading_partner_view()
            bpc_exchange_rates_df = self._extract_bpc_exchange_rates()
            
            # Stage 2: Apply transformations
            transformed_df = self._transform_data(
                faglflexa_df, 
                bseg_df,
                entity_golden_view_df,
                golden_gl_view_df,
                golden_trading_partner_df,
                bpc_exchange_rates_df
            )
            
            # Stage 3: Determine offset account
            finance_df = self._determine_offset_account(transformed_df)
            
            # Stage 4: Load data to Finance table
            self._load_data(finance_df)
            
            logger.info("Finance data processing job completed successfully")
            
        except Exception as e:
            logger.error(f"Error in finance data processing job: {str(e)}", exc_info=True)
            raise
    
    def _extract_with_retry(self, extract_func, max_retries=MAX_RETRIES) -> DataFrame:
        """
        Extract data with retry mechanism.
        
        Args:
            extract_func: Function to extract data
            max_retries: Maximum number of retries
            
        Returns:
            DataFrame: Extracted data
        """
        import time
        
        retries = 0
        last_exception = None
        
        while retries < max_retries:
            try:
                return extract_func()
            except Exception as e:
                last_exception = e
                retries += 1
                logger.warning(f"Extraction attempt {retries} failed: {str(e)}")
                if retries < max_retries:
                    logger.info(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS)
        
        logger.error(f"All {max_retries} extraction attempts failed")
        raise last_exception
    
    def _extract_faglflexa_data(self) -> DataFrame:
        """
        Extract data from FAGLFLEXA table in ECC Everest.
        
        Returns:
            DataFrame: FAGLFLEXA data
        """
        logger.info("Extracting data from FAGLFLEXA table")
        
        def _extract():
            # In a real implementation, this would use JDBC or another connector to extract from ECC
            # For this example, we'll simulate the extraction
            faglflexa_schema = StructType([
                StructField("RYEAR", StringType(), False),
                StructField("POPER", StringType(), False),
                StructField("DOCNR", StringType(), False),
                StructField("RBUKRS", StringType(), False),  # CompCode
                StructField("RACCT", StringType(), False),   # GLAccount
                StructField("RCNTR", StringType(), True),    # CostCenter
                StructField("PRCTR", StringType(), True),    # ProfitCenter
                StructField("RFAREA", StringType(), True),   # FunctionalArea
                StructField("RBUSA", StringType(), True),    # BusinessArea
                StructField("KOKRS", StringType(), True),    # ControllingArea
                StructField("SEGMENT", StringType(), True),  # Segment
                StructField("SCNTR", StringType(), True),    # SenderCostCenter
                StructField("PPRCTR", StringType(), True),   # SenderProfitCenter
                StructField("SFAREA", StringType(), True),   # SenderFunctionalArea
                StructField("SBUSA", StringType(), True),    # SenderBusinessArea
                StructField("RASSC", StringType(), True),    # TradingPartner
                StructField("HSLVT", DecimalType(17, 2), True),  # AmountLC
                StructField("HSL", DecimalType(17, 2), True),    # AmountLC YTD
                StructField("RHCUR", StringType(), True),    # LocalCurrency
                StructField("RKCUR", StringType(), True),    # GlobalCurrency
                StructField("KSLVT", DecimalType(17, 2), True),  # AmountGC
                StructField("KSL", DecimalType(17, 2), True),    # AmountGC YTD
                StructField("DRCRK", StringType(), True),    # DebitCreditIndicator
            ])
            
            # Simulate data extraction with sample data
            return self.spark.createDataFrame(
                self.spark.sparkContext.parallelize([
                    # Sample data would go here in a real implementation
                    (str(self.fiscal_year), str(self.posting_period), "1000000001", "1000", "100000", "CC001", "PC001", 
                     "FA001", "BA001", "CO01", "SEG01", "SCC001", "SPC001", "SFA001", "SBA001", "TP001", 
                     1000.00, 5000.00, "USD", "EUR", 900.00, 4500.00, "S"),
                    # Additional sample records...
                ]),
                faglflexa_schema
            )
        
        return self._extract_with_retry(_extract)
    
    def _extract_bseg_data(self) -> DataFrame:
        """
        Extract data from BSEG table in ECC Everest.
        
        Returns:
            DataFrame: BSEG data
        """
        logger.info("Extracting data from BSEG table")
        
        def _extract():
            # In a real implementation, this would use JDBC or another connector to extract from ECC
            bseg_schema = StructType([
                StructField("GJAHR", StringType(), False),   # Fiscal Year
                StructField("MONAT", StringType(), False),   # Posting Period
                StructField("BELNR", StringType(), False),   # Document Number
                StructField("BUZEI", StringType(), False),   # Line Item
                StructField("BUKRS", StringType(), False),   # Company Code
                StructField("HKONT", StringType(), False),   # GL Account
                StructField("AUGDT", DateType(), True),      # Clearing Date
                StructField("AUGBL", StringType(), True),    # Clearing Document
                StructField("ZUONR", StringType(), True),    # Assignment
                StructField("SGTXT", StringType(), True),    # Item Text
                StructField("WRBTR", DecimalType(17, 2), True),  # Amount in Document Currency
                StructField("DMBTR", DecimalType(17, 2), True),  # Amount in Local Currency
                StructField("WRBTR_GC", DecimalType(17, 2), True),  # Amount in Global Currency (simulated)
            ])
            
            # Simulate data extraction with sample data
            return self.spark.createDataFrame(
                self.spark.sparkContext.parallelize([
                    # Sample data would go here in a real implementation
                    (str(self.fiscal_year), str(self.posting_period), "1000000001", "001", "1000", "100000", 
                     datetime(2023, 1, 15), "1000000002", "ASSIGN001", "Payment for services", 
                     1000.00, 1000.00, 900.00),
                    # Additional sample records...
                ]),
                bseg_schema
            )
        
        return self._extract_with_retry(_extract)
    
    def _extract_entity_golden_view(self) -> DataFrame:
        """
        Extract data from Entity golden view.
        
        Returns:
            DataFrame: Entity golden view data
        """
        logger.info("Extracting data from Entity golden view")
        
        def _extract():
            # In a real implementation, this would query the data warehouse
            entity_schema = StructType([
                StructField("CompCode", StringType(), False),
                StructField("LegalEntity", StringType(), False),
            ])
            
            # Simulate data extraction with sample data
            return self.spark.createDataFrame(
                self.spark.sparkContext.parallelize([
                    ("1000", "LE001"),
                    ("2000", "LE002"),
                    # Additional sample records...
                ]),
                entity_schema
            )
        
        return self._extract_with_retry(_extract)
    
    def _extract_golden_gl_view(self) -> DataFrame:
        """
        Extract data from Golden GL view.
        
        Returns:
            DataFrame: Golden GL view data
        """
        logger.info("Extracting data from Golden GL view")
        
        def _extract():
            # In a real implementation, this would query the data warehouse
            gl_schema = StructType([
                StructField("GLAccount", StringType(), False),
                StructField("GoldenGLAcct", StringType(), False),
            ])
            
            # Simulate data extraction with sample data
            return self.spark.createDataFrame(
                self.spark.sparkContext.parallelize([
                    ("100000", "G100000"),
                    ("200000", "G200000"),
                    # Additional sample records...
                ]),
                gl_schema
            )
        
        return self._extract_with_retry(_extract)
    
    def _extract_golden_trading_partner_view(self) -> DataFrame:
        """
        Extract data from Golden Trading Partner view.
        
        Returns:
            DataFrame: Golden Trading Partner view data
        """
        logger.info("Extracting data from Golden Trading Partner view")
        
        def _extract():
            # In a real implementation, this would query the data warehouse
            tp_schema = StructType([
                StructField("TradingPartner", StringType(), False),
                StructField("GoldenTradingPartner", StringType(), False),
            ])
            
            # Simulate data extraction with sample data
            return self.spark.createDataFrame(
                self.spark.sparkContext.parallelize([
                    ("TP001", "GTP001"),
                    ("TP002", "GTP002"),
                    # Additional sample records...
                ]),
                tp_schema
            )
        
        return self._extract_with_retry(_extract)
    
    def _extract_bpc_exchange_rates(self) -> DataFrame:
        """
        Extract data from BPC exchange rates view.
        
        Returns:
            DataFrame: BPC exchange rates data
        """
        logger.info("Extracting data from BPC exchange rates view")
        
        def _extract():
            # In a real implementation, this would query the data warehouse
            rates_schema = StructType([
                StructField("FiscalYear", IntegerType(), False),
                StructField("Period", IntegerType(), False),
                StructField("FromCurrency", StringType(), False),
                StructField("ToCurrency", StringType(), False),
                StructField("Rate", DecimalType(17, 6), False),
            ])
            
            # Simulate data extraction with sample data
            return self.spark.createDataFrame(
                self.spark.sparkContext.parallelize([
                    (self.fiscal_year, self.posting_period, "USD", "EUR", 0.9),
                    (self.fiscal_year, self.posting_period, "EUR", "USD", 1.11),
                    (self.fiscal_year, self.posting_period, "JPY", "USD", 0.0069),
                    # Additional sample records...
                ]),
                rates_schema
            )
        
        return self._extract_with_retry(_extract)
    
    def _transform_data(
        self,
        faglflexa_df: DataFrame,
        bseg_df: DataFrame,
        entity_golden_view_df: DataFrame,
        golden_gl_view_df: DataFrame,
        golden_trading_partner_df: DataFrame,
        bpc_exchange_rates_df: DataFrame
    ) -> DataFrame:
        """
        Apply transformations to the extracted data.
        
        Args:
            faglflexa_df: FAGLFLEXA data
            bseg_df: BSEG data
            entity_golden_view_df: Entity golden view data
            golden_gl_view_df: Golden GL view data
            golden_trading_partner_df: Golden Trading Partner view data
            bpc_exchange_rates_df: BPC exchange rates data
            
        Returns:
            DataFrame: Transformed data
        """
        logger.info("Applying transformations to extracted data")
        
        try:
            # Step 1: Map source fields to target fields from FAGLFLEXA
            transformed_df = faglflexa_df.select(
                F.col("RYEAR").alias("SourceFiscalYear"),
                F.col("POPER").alias("SourcePeriod"),
                F.col("DOCNR").alias("DocumentNumber"),
                F.col("RBUKRS").alias("CompCode"),
                F.col("RACCT").alias("GLAccount"),
                F.col("RCNTR").alias("CostCenter"),
                F.col("PRCTR").alias("ProfitCenter"),
                F.col("RFAREA").alias("FunctionalArea"),
                F.col("RBUSA").alias("BusinessArea"),
                F.col("KOKRS").alias("ControllingArea"),
                F.col("SEGMENT").alias("Segment"),
                F.col("SCNTR").alias("SenderCostCenter"),
                F.col("PPRCTR").alias("SenderProfitCenter"),
                F.col("SFAREA").alias("SenderFunctionalArea"),
                F.col("SBUSA").alias("SenderBusinessArea"),
                F.col("RASSC").alias("TradingPartner"),
                F.col("HSLVT").alias("AmountLC"),
                F.col("RHCUR").alias("LocalCurrency"),
                F.col("RKCUR").alias("GlobalCurrency"),
                F.col("KSLVT").alias("AmountGC"),
                F.col("DRCRK").alias("DebitCreditIndicator"),
                F.lit(self.fiscal_year).alias("FiscalYear"),
                F.lit(self.posting_period).alias("PostingPeriod")
            )
            
            # Step 2: Join with BSEG to get additional fields
            transformed_df = transformed_df.join(
                bseg_df.select(
                    F.col("BELNR").alias("DocumentNumber"),
                    F.col("BUZEI").alias("LineItem"),
                    F.col("AUGDT").alias("ClearingDate"),
                    F.col("AUGBL").alias("ClearingDocument"),
                    F.col("ZUONR").alias("Assignment"),
                    F.col("SGTXT").alias("ItemText")
                ),
                "DocumentNumber",
                "left"
            )
            
            # Step 3: Join with entity golden view to get LegalEntity
            transformed_df = transformed_df.join(
                entity_golden_view_df,
                "CompCode",
                "left"
            )
            
            # Step 4: Join with golden GL view to get GoldenGLAcct
            transformed_df = transformed_df.join(
                golden_gl_view_df,
                "GLAccount",
                "left"
            )
            
            # Step 5: Join with golden trading partner view to get GoldenTradingPartner
            transformed_df = transformed_df.join(
                golden_trading_partner_df,
                "TradingPartner",
                "left"
            )
            
            # Step 6: Calculate GainLossGC and GainLossLC
            # First, join with exchange rates
            transformed_df = transformed_df.join(
                bpc_exchange_rates_df.filter(
                    (F.col("FiscalYear") == self.fiscal_year) & 
                    (F.col("Period") == self.posting_period)
                ).select(
                    F.col("FromCurrency"),
                    F.col("ToCurrency"),
                    F.col("Rate")
                ),
                (transformed_df["LocalCurrency"] == bpc_exchange_rates_df["FromCurrency"]) &
                (transformed_df["GlobalCurrency"] == bpc_exchange_rates_df["ToCurrency"]),
                "left"
            )
            
            # Calculate GainLossGC
            transformed_df = transformed_df.withColumn(
                "CalculatedAmountGC", 
                F.when(
                    F.col("Rate").isNotNull(),
                    F.col("AmountLC") * F.col("Rate")
                ).otherwise(F.lit(None))
            )
            
            transformed_df = transformed_df.withColumn(
                "GainLossGC",
                F.when(
                    F.col("CalculatedAmountGC").isNotNull(),
                    F.col("AmountGC") - F.col("CalculatedAmountGC")
                ).otherwise(F.lit(0))
            )
            
            # Apply multiplication factor for specific currencies
            transformed_df = transformed_df.withColumn(
                "GainLossLC",
                F.when(
                    F.col("LocalCurrency").isin(LOCAL_CURRENCY_MULT_FACTOR),
                    F.col("GainLossGC") * 100
                ).otherwise(F.col("GainLossGC"))
            )
            
            # Clean up temporary columns
            transformed_df = transformed_df.drop("CalculatedAmountGC", "FromCurrency", "ToCurrency", "Rate")
            
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error in data transformation: {str(e)}", exc_info=True)
            raise
    
    def _determine_offset_account(self, df: DataFrame) -> DataFrame:
        """
        Determine offset account for each transaction.
        
        Args:
            df: Transformed data
            
        Returns:
            DataFrame: Data with offset account determined
        """
        logger.info("Determining offset account")
        
        try:
            # Group by document to find offset accounts
            window_spec = Window.partitionBy("DocumentNumber")
            
            # First, identify all accounts in each document
            doc_accounts = df.select(
                "DocumentNumber",
                "GLAccount"
            ).distinct()
            
            # For each document, collect all accounts as an array
            doc_accounts_agg = doc_accounts.groupBy("DocumentNumber").agg(
                F.collect_list("GLAccount").alias("DocumentAccounts")
            )
            
            # Join back to the main dataframe
            df_with_doc_accounts = df.join(doc_accounts_agg, "DocumentNumber", "left")
            
            # Determine offset account - in a real implementation, this would use more complex logic
            # For this example, we'll use a simple approach: the offset is the first different account in the document
            df_with_offset = df_with_doc_accounts.withColumn(
                "OffsetAccount",
                F.expr("""
                    CASE 
                        WHEN size(DocumentAccounts) > 1 THEN 
                            CASE 
                                WHEN DocumentAccounts[0] = GLAccount THEN DocumentAccounts[1]
                                ELSE DocumentAccounts[0]
                            END
                        ELSE NULL
                    END
                """)
            )
            
            # Clean up temporary columns
            result_df = df_with_offset.drop("DocumentAccounts")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error in determining offset account: {str(e)}", exc_info=True)
            raise
    
    def _load_data(self, df: DataFrame) -> None:
        """
        Load transformed data into Finance table.
        
        Args:
            df: Transformed data to load
        """
        logger.info("Loading transformed data into Finance table")
        
        try:
            # In a real implementation, this would write to the data warehouse
            # For this example, we'll simulate the write operation
            
            # Count records for logging
            record_count = df.count()
            logger.info(f"Loading {record_count} records into Finance table")
            
            # Simulate write operation
            # df.write.mode("append").jdbc(url, "Finance", properties)
            
            # For this example, we'll just show the data
            df.show(5, truncate=False)
            
            logger.info(f"Successfully loaded {record_count} records into Finance table")
            
        except Exception as e:
            logger.error(f"Error in data loading: {str(e)}", exc_info=True)
            raise


def run_finance_data_processor(spark: SparkSession, fiscal_year: int, posting_period: int) -> None:
    """
    Run the Finance Data Processor job.
    
    Args:
        spark: SparkSession to use
        fiscal_year: Fiscal year to process
        posting_period: Posting period to process
    """
    processor = FinanceDataProcessor(spark, fiscal_year, posting_period)
    processor.run()


if __name__ == "__main__":
    # This section would be executed when running the script directly
    
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Finance Data Processing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("INFO")
    
    # Get parameters (in production, these would come from arguments or job parameters)
    fiscal_year = 2023
    posting_period = 12
    
    # Run the processor
    run_finance_data_processor(spark, fiscal_year, posting_period)
    
    # Stop SparkSession
    spark.stop()