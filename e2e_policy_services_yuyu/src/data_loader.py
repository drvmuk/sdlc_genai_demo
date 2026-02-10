"""
Data loader module for reading source and lookup tables from Oracle.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim
from config import Config


class DataLoader:
    """Handles loading data from Oracle source and lookup tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = Config()
    
    def load_staging_data(self) -> DataFrame:
        """
        Load source data from STG_E2E_AC_TXDBH_DATA.
        
        Returns:
            DataFrame with staging data
        """
        df = self.spark.read.jdbc(
            url=self.config.get_oracle_jdbc_url(),
            table=self.config.SOURCE_TABLE,
            properties=self.config.get_oracle_properties()
        )
        
        # Select required columns based on S2T mapping
        required_cols = [
            "T_TX_REQUEST_REQUEST_ID",
            "T_TX_BASIC_TRANS_ID",
            "T_TX_RELATION_TRANS_REL_ID",
            "T_TX_REQ_POL_REQUEST_POLICY_ID",
            "STG_POLICY_ID",
            "STG_TXDB_STATUS",
            "T_TX_REQUEST_ORIGIN_REQUEST_ID",
            "T_TX_BASIC_POLICY_ID",
            "T_TX_REQUEST_REQ_ACC_DATETIME",
            "T_TX_DTL_ADD_CH_N_TRANS_ZIP",
            "T_TX_DTL_ADD_CH_N_TRANS_ADD1",
            "T_TX_DTL_ADD_CH_N_TRANS_AD1_KJ",
            "T_TX_DTL_ADD_CH_N_TRANS_ADD2",
            "T_TX_DTL_ADD_CH_N_TRANS_AD2_KJ",
            "T_TX_DTL_ADD_CH_N_TRANS_ADD3",
            "T_TX_DTL_ADD_CH_N_TRANS_AD3_KJ",
            "T_TX_DTL_ADD_CH_N_TRANS_PHNO",
            "PPAY_ZIP",
            "PPAY_ADR1_FW",
            "PPAY_ADR2_FW",
            "PPAY_ADR3_FW"
        ]
        
        return df.select(*[col(c) for c in required_cols if c in df.columns])
    
    def load_lookup_yuyu_clnt(self) -> DataFrame:
        """
        Load lookup data from T_YUYU_CLNT.
        
        Returns:
            DataFrame with policy owner name components (Kana)
        """
        df = self.spark.read.jdbc(
            url=self.config.get_oracle_jdbc_url(),
            table=self.config.LOOKUP_TABLE_YUYU_CLNT,
            properties=self.config.get_oracle_properties()
        )
        
        return df.select(
            col("POL_NO").alias("LKP_POL_NO_CLNT"),
            col("POWN_LNM"),
            col("POWN_FNM")
        )
    
    def load_lookup_yuyuk_cln(self) -> DataFrame:
        """
        Load lookup data from T_YUYUK_CLN.
        
        Returns:
            DataFrame with policy owner name (Kanji)
        """
        df = self.spark.read.jdbc(
            url=self.config.get_oracle_jdbc_url(),
            table=self.config.LOOKUP_TABLE_YUYUK_CLN,
            properties=self.config.get_oracle_properties()
        )
        
        return df.select(
            col("POL_NO").alias("LKP_POL_NO_CLN"),
            col("POWN_KNM")
        )
    
    def enrich_with_lookups(
        self, 
        source_df: DataFrame, 
        lookup_clnt_df: DataFrame, 
        lookup_cln_df: DataFrame
    ) -> DataFrame:
        """
        Enrich source data with lookup information.
        
        Args:
            source_df: Source staging DataFrame
            lookup_clnt_df: YUYU_CLNT lookup DataFrame
            lookup_cln_df: YUYUK_CLN lookup DataFrame
        
        Returns:
            Enriched DataFrame
        """
        # Left join with T_YUYU_CLNT (BR-09)
        enriched_df = source_df.join(
            lookup_clnt_df,
            source_df.T_TX_BASIC_POLICY_ID == lookup_clnt_df.LKP_POL_NO_CLNT,
            "left"
        ).drop("LKP_POL_NO_CLNT")
        
        # Left join with T_YUYUK_CLN (BR-09)
        enriched_df = enriched_df.join(
            lookup_cln_df,
            source_df.T_TX_BASIC_POLICY_ID == lookup_cln_df.LKP_POL_NO_CLN,
            "left"
        ).drop("LKP_POL_NO_CLN")
        
        return enriched_df