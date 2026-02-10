"""
Business rule transformations for YUYU address change extract.
Implements BR-01 through BR-08.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, length, trim, concat, lit, regexp_replace,
    date_format, to_timestamp, current_timestamp, substring,
    coalesce, row_number
)
from pyspark.sql.window import Window
from config import Config


class YUYUTransformations:
    """Implements business rule transformations for YUYU extract."""
    
    def __init__(self):
        self.config = Config()
    
    def apply_all_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply all business rule transformations.
        
        Args:
            df: Enriched source DataFrame
        
        Returns:
            Transformed DataFrame ready for output
        """
        df = self._apply_reception_date_formatting(df)  # BR-03
        df = self._apply_address_fallback_logic(df)     # BR-04
        df = self._apply_postal_code_normalization(df)  # BR-05
        df = self._apply_phone_formatting(df)           # BR-06
        df = self._apply_owner_name_derivation(df)      # BR-07, BR-08
        df = self._apply_process_stamps(df)             # BR-01, BR-02
        df = self._add_sequence_number(df)
        
        return df
    
    def _apply_reception_date_formatting(self, df: DataFrame) -> DataFrame:
        """
        BR-03: Reception date formatting and conversion.
        Note: Addressing format mask inconsistency by using consistent format.
        """
        return df.withColumn(
            "RECEPTION_DATE",
            to_timestamp(
                date_format(col("T_TX_REQUEST_REQ_ACC_DATETIME"), "yyyy-MM-dd HH:mm"),
                "yyyy-MM-dd HH:mm"
            )
        )
    
    def _apply_address_fallback_logic(self, df: DataFrame) -> DataFrame:
        """
        BR-04: New address (Kana) fallback to PPAY_* when ZIP is missing.
        """
        # Check if ZIP is missing (NULL, empty, or length 0)
        zip_missing = (
            col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull() |
            (trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == "") |
            (length(trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))) == 0)
        )
        
        df = df.withColumn(
            "NEW_ADDRESS_KANA_1",
            when(zip_missing, col("PPAY_ADR1_FW"))
            .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD1"))
        )
        
        df = df.withColumn(
            "NEW_ADDRESS_KANA_2",
            when(zip_missing, col("PPAY_ADR2_FW"))
            .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD2"))
        )
        
        df = df.withColumn(
            "NEW_ADDRESS_KANA_3",
            when(zip_missing, col("PPAY_ADR3_FW"))
            .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD3"))
        )
        
        # Kanji addresses pass through directly
        df = df.withColumn("NEW_ADDRESS_KANJI_1", col("T_TX_DTL_ADD_CH_N_TRANS_AD1_KJ"))
        df = df.withColumn("NEW_ADDRESS_KANJI_2", col("T_TX_DTL_ADD_CH_N_TRANS_AD2_KJ"))
        df = df.withColumn("NEW_ADDRESS_KANJI_3", col("T_TX_DTL_ADD_CH_N_TRANS_AD3_KJ"))
        
        return df
    
    def _apply_postal_code_normalization(self, df: DataFrame) -> DataFrame:
        """
        BR-05: Postal code fallback and normalization (remove hyphens).
        """
        # Fallback to PPAY_ZIP if main ZIP is missing
        zip_missing = (
            col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull() |
            (trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == "") |
            (length(trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))) == 0)
        )
        
        df = df.withColumn(
            "v_zip",
            when(zip_missing, col("PPAY_ZIP"))
            .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))
        )
        
        # Remove hyphens
        df = df.withColumn(
            "NEW_ADDRESS_POSTAL_CODE",
            regexp_replace(coalesce(col("v_zip"), lit("")), "-", "")
        )
        
        return df.drop("v_zip")
    
    def _apply_phone_formatting(self, df: DataFrame) -> DataFrame:
        """
        BR-06: Telephone number formatting.
        Format: AAA-BBBB-CCCCCC for mobile (050/060/070/080/090)
                AA-BBBB-CCCCCC for landline
        """
        phone_col = trim(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"))
        
        # Check if NULL or blank
        phone_is_blank = phone_col.isNull() | (phone_col == "")
        
        # Extract prefix (first 3 chars)
        prefix = substring(phone_col, 1, 3)
        is_mobile = prefix.isin(["050", "060", "070", "080", "090"])
        
        # Mobile format: AAA-BBBB-CCCCCC
        mobile_format = concat(
            substring(phone_col, 1, 3),
            lit("-"),
            substring(phone_col, 4, 4),
            lit("-"),
            substring(phone_col, 8, 6)
        )
        
        # Landline format: AA-BBBB-CCCCCC
        landline_format = concat(
            substring(phone_col, 1, 2),
            lit("-"),
            substring(phone_col, 3, 4),
            lit("-"),
            substring(phone_col, 7, 6)
        )
        
        df = df.withColumn(
            "NEW_ADDRESS_TELEPHONE_NUMBER",
            when(phone_is_blank, lit(""))
            .when(is_mobile, mobile_format)
            .otherwise(landline_format)
        )
        
        return df
    
    def _apply_owner_name_derivation(self, df: DataFrame) -> DataFrame:
        """
        BR-07: Policy owner name derivation (Kana).
        BR-08: Policy owner name derivation (Kanji).
        """
        # BR-07: Kana name from T_YUYU_CLNT
        first_name_blank = (
            col("POWN_FNM").isNull() |
            (trim(col("POWN_FNM")) == "")
        )
        
        df = df.withColumn(
            "POLICY_OWNER_NAME_KANA",
            when(
                first_name_blank,
                trim(col("POWN_LNM"))
            ).otherwise(
                concat(
                    trim(col("POWN_LNM")),
                    lit(" "),
                    trim(col("POWN_FNM"))
                )
            )
        )
        
        # BR-08: Kanji name from T_YUYUK_CLN
        df = df.withColumn(
            "POLICY_OWNER_NAME_KANJI",
            col("POWN_KNM")
        )
        
        return df
    
    def _apply_process_stamps(self, df: DataFrame) -> DataFrame:
        """
        BR-01: Stamp processing date (SYSDATE).
        BR-02: Stamp processing user.
        """
        df = df.withColumn("STG_PROCESS_DATE", current_timestamp())  # BR-01
        df = df.withColumn("STG_PROCESS_USERID", lit(self.config.M_PROCESS_USERID))  # BR-02
        
        return df
    
    def _add_sequence_number(self, df: DataFrame) -> DataFrame:
        """
        Add sequence number starting from 0 with increment 1.
        """
        window_spec = Window.orderBy(lit(1))
        
        df = df.withColumn(
            "SEQUENCE_NUMBER",
            row_number().over(window_spec) - 1
        )
        
        return df
    
    def select_flat_file_columns(self, df: DataFrame) -> DataFrame:
        """
        Select and order columns for DPAddressChangeYUYU output.
        
        Args:
            df: Transformed DataFrame
        
        Returns:
            DataFrame with flat file schema
        """
        return df.select(
            col("SEQUENCE_NUMBER").cast("int"),
            col("T_TX_REQUEST_ORIGIN_REQUEST_ID").alias("ORIGIN_REQUEST_ID"),
            col("T_TX_BASIC_POLICY_ID").alias("POLICY_ID"),
            col("RECEPTION_DATE"),
            col("NEW_ADDRESS_POSTAL_CODE"),
            col("NEW_ADDRESS_KANA_1"),
            col("NEW_ADDRESS_KANJI_1"),
            col("NEW_ADDRESS_KANA_2"),
            col("NEW_ADDRESS_KANJI_2"),
            col("NEW_ADDRESS_KANA_3"),
            col("NEW_ADDRESS_KANJI_3"),
            col("NEW_ADDRESS_TELEPHONE_NUMBER"),
            col("POLICY_OWNER_NAME_KANA"),
            col("POLICY_OWNER_NAME_KANJI")
        )
    
    def select_staging_update_columns(self, df: DataFrame) -> DataFrame:
        """
        Select columns for staging table update.
        
        Args:
            df: Transformed DataFrame
        
        Returns:
            DataFrame with staging update schema
        """
        return df.select(
            col("T_TX_REQUEST_REQUEST_ID"),
            col("T_TX_BASIC_TRANS_ID"),
            col("T_TX_RELATION_TRANS_REL_ID"),
            col("T_TX_REQ_POL_REQUEST_POLICY_ID"),
            col("STG_POLICY_ID"),
            col("STG_TXDB_STATUS"),
            col("STG_PROCESS_DATE"),
            col("STG_PROCESS_USERID")
        )