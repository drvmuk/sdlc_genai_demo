"""
E2E Address Change YUYU Processing Module

This module processes address change records from the staging table,
creates a flat file extract, and updates the staging table with processing metadata.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, trim, regexp_replace, substring, concat, concat_ws,
    to_date, to_timestamp, length, when, udf, monotonically_increasing_id
)
from pyspark.sql.types import StringType, TimestampType
from datetime import datetime
import os


class AddressChangeProcessor:
    """Processes address change records for YUYU output and staging updates."""
    
    def __init__(self, spark: SparkSession, process_userid: str):
        """
        Initialize the processor.
        
        Args:
            spark: Active SparkSession
            process_userid: User ID to stamp on processed records
        """
        self.spark = spark
        self.process_userid = process_userid
        self.current_timestamp = datetime.now()
    
    def read_staging_data(self, table_name: str) -> DataFrame:
        """
        Read data from the staging table.
        
        Args:
            table_name: Full name of the staging table
            
        Returns:
            DataFrame containing staging data
        """
        return self.spark.table(table_name)
    
    def apply_lookups(self, df: DataFrame) -> DataFrame:
        """
        Apply lookups to enrich the data with policy owner information.
        
        Args:
            df: Source DataFrame
            
        Returns:
            DataFrame with lookup data added
        """
        # Lookup YUYU_CLNT for policy owner name components (Kana)
        yuyu_clnt_df = self.spark.table("T_YUYU_CLNT")
        df_with_owner = df.join(
            yuyu_clnt_df,
            df["T_TX_BASIC_POLICY_ID"] == yuyu_clnt_df["POL_NO"],
            "left_outer"
        ).select(
            df["*"],
            yuyu_clnt_df["POWN_LNM"],
            yuyu_clnt_df["POWN_FNM"]
        )
        
        # Lookup YUYUK_CLN for policy owner Kanji name
        yuyuk_cln_df = self.spark.table("T_YUYUK_CLN")
        result_df = df_with_owner.join(
            yuyuk_cln_df,
            df_with_owner["T_TX_BASIC_POLICY_ID"] == yuyuk_cln_df["POL_NO"],
            "left_outer"
        ).select(
            df_with_owner["*"],
            yuyuk_cln_df["POWN_KNM"]
        )
        
        return result_df
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply business transformations to the data.
        
        Args:
            df: DataFrame with source and lookup data
            
        Returns:
            DataFrame with all transformations applied
        """
        # BR-03: Reception date formatting and conversion
        # BR-04: New address (Kana) fallback to PPAY_* when ZIP is missing
        # BR-05: Postal code fallback and normalization
        # BR-06: Telephone number formatting
        # BR-07: Policy owner name derivation (Kana)
        # BR-08: Policy owner name derivation (Kanji)
        
        return df.withColumn(
            "O__REQUEST_ACCEPT_DATETIME", 
            to_timestamp(
                to_date(col("T_TX_REQUEST_REQ_ACC_DATETIME"), "yyyy/MM/dd HH:mm"),
                "yyyy-MM-dd HH:mm"
            )
        ).withColumn(
            "v_T_TX_DTL_ADD_CH_N_TRANS_ZIP",
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
                (trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == "") | 
                (length(trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))) == 0),
                col("PPAY_ZIP")
            ).otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))
        ).withColumn(
            "o_NEW_ZIP",
            regexp_replace(col("v_T_TX_DTL_ADD_CH_N_TRANS_ZIP"), "-", "")
        ).withColumn(
            "o_T_TX_DTL_ADD_CH_N_TRANS_ADD1",
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
                (trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == "") | 
                (length(trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))) == 0),
                col("PPAY_ADR1_FW")
            ).otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD1"))
        ).withColumn(
            "o_T_TX_DTL_ADD_CH_N_TRANS_ADD2",
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
                (trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == "") | 
                (length(trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))) == 0),
                col("PPAY_ADR2_FW")
            ).otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD2"))
        ).withColumn(
            "o_T_TX_DTL_ADD_CH_N_TRANS_ADD3",
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
                (trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == "") | 
                (length(trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))) == 0),
                col("PPAY_ADR3_FW")
            ).otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD3"))
        ).withColumn(
            "O_NEW_PHONE_NUMBER",
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_PHNO").isNull()) | 
                (trim(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO")) == ""),
                lit("")
            ).when(
                substring(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"), 1, 3).isin("050", "060", "070", "080", "090"),
                concat(
                    substring(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"), 1, 3),
                    lit("-"),
                    substring(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"), 4, 4),
                    lit("-"),
                    substring(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"), 8, 6)
                )
            ).otherwise(
                concat(
                    substring(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"), 1, 2),
                    lit("-"),
                    substring(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"), 3, 4),
                    lit("-"),
                    substring(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"), 7, 6)
                )
            )
        ).withColumn(
            "O_KANA_NAME",
            when(
                (col("POWN_FNM").isNull()) | (trim(col("POWN_FNM")) == ""),
                trim(col("POWN_LNM"))
            ).otherwise(
                concat_ws(" ", trim(col("POWN_LNM")), trim(col("POWN_FNM")))
            )
        ).withColumn(
            "o_STG_PROCESS_DATE",
            lit(self.current_timestamp)
        ).withColumn(
            "o_STG_PROCESS_USERID",
            lit(self.process_userid)
        )
    
    def prepare_yuyu_extract(self, df: DataFrame) -> DataFrame:
        """
        Prepare the YUYU extract dataset.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame ready for YUYU extract
        """
        # Generate sequence number
        return df.withColumn(
            "SEQUENCE_NUMBER", 
            monotonically_increasing_id()
        ).select(
            col("SEQUENCE_NUMBER"),
            col("T_TX_REQUEST_ORIGIN_REQUEST_ID").alias("ORIGIN_REQUEST_ID"),
            col("T_TX_BASIC_POLICY_ID").alias("POLICY_ID"),
            col("O__REQUEST_ACCEPT_DATETIME").alias("RECEPTION_DATE"),
            col("o_NEW_ZIP").alias("NEW_ADDRESS_POSTAL_CODE"),
            col("o_T_TX_DTL_ADD_CH_N_TRANS_ADD1").alias("NEW_ADDRESS_KANA_1"),
            col("T_TX_DTL_ADD_CH_N_TRANS_AD1_KJ").alias("NEW_ADDRESS_KANJI_1"),
            col("o_T_TX_DTL_ADD_CH_N_TRANS_ADD2").alias("NEW_ADDRESS_KANA_2"),
            col("T_TX_DTL_ADD_CH_N_TRANS_AD2_KJ").alias("NEW_ADDRESS_KANJI_2"),
            col("o_T_TX_DTL_ADD_CH_N_TRANS_ADD3").alias("NEW_ADDRESS_KANA_3"),
            col("T_TX_DTL_ADD_CH_N_TRANS_AD3_KJ").alias("NEW_ADDRESS_KANJI_3"),
            col("O_NEW_PHONE_NUMBER").alias("NEW_ADDRESS_TELEPHONE_NUMBER"),
            col("O_KANA_NAME").alias("POLICY_OWNER_NAME_KANA"),
            col("POWN_KNM").alias("POLICY_OWNER_NAME_KANJI")
        )
    
    def prepare_staging_update(self, df: DataFrame) -> DataFrame:
        """
        Prepare the staging update dataset.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame ready for staging update
        """
        # Select only the columns needed for the update
        return df.select(
            # Key identifiers retained
            col("T_TX_REQUEST_REQUEST_ID"),
            col("T_TX_BASIC_TRANS_ID"),
            col("T_TX_RELATION_TRANS_REL_ID"),
            col("T_TX_REQ_POL_REQUEST_POLICY_ID"),
            col("STG_POLICY_ID"),
            col("STG_TXDB_STATUS"),
            # Process stamps
            col("o_STG_PROCESS_DATE").alias("STG_PROCESS_DATE"),
            col("o_STG_PROCESS_USERID").alias("STG_PROCESS_USERID")
        )
    
    def write_yuyu_extract(self, df: DataFrame, output_path: str) -> None:
        """
        Write the YUYU extract to a file.
        
        Args:
            df: DataFrame with YUYU extract data
            output_path: Path to write the output file
        """
        df.write.mode("overwrite").option("header", "true").csv(output_path)
    
    def update_staging_table(self, df: DataFrame, table_name: str) -> None:
        """
        Update the staging table with processing metadata.
        
        Args:
            df: DataFrame with staging update data
            table_name: Name of the staging table to update
        """
        # Create a temporary view for the update data
        update_view_name = f"temp_update_view_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        df.createOrReplaceTempView(update_view_name)
        
        # Perform the update using SQL
        self.spark.sql(f"""
            MERGE INTO {table_name} target
            USING {update_view_name} source
            ON target.T_TX_REQUEST_REQUEST_ID = source.T_TX_REQUEST_REQUEST_ID
                AND target.T_TX_BASIC_TRANS_ID = source.T_TX_BASIC_TRANS_ID
                AND target.T_TX_RELATION_TRANS_REL_ID = source.T_TX_RELATION_TRANS_REL_ID
                AND target.T_TX_REQ_POL_REQUEST_POLICY_ID = source.T_TX_REQ_POL_REQUEST_POLICY_ID
                AND target.STG_POLICY_ID = source.STG_POLICY_ID
            WHEN MATCHED THEN
                UPDATE SET
                    target.STG_PROCESS_DATE = source.STG_PROCESS_DATE,
                    target.STG_PROCESS_USERID = source.STG_PROCESS_USERID
        """)
    
    def process(self, source_table: str, target_table: str, output_path: str) -> None:
        """
        Execute the full processing pipeline.
        
        Args:
            source_table: Name of the source staging table
            target_table: Name of the target staging table (same as source)
            output_path: Path to write the YUYU extract
        """
        # Read source data
        source_df = self.read_staging_data(source_table)
        
        # Apply lookups
        enriched_df = self.apply_lookups(source_df)
        
        # Apply transformations
        transformed_df = self.transform_data(enriched_df)
        
        # Prepare YUYU extract
        yuyu_extract_df = self.prepare_yuyu_extract(transformed_df)
        
        # Prepare staging update
        staging_update_df = self.prepare_staging_update(transformed_df)
        
        # Write YUYU extract
        self.write_yuyu_extract(yuyu_extract_df, output_path)
        
        # Update staging table
        self.update_staging_table(staging_update_df, target_table)