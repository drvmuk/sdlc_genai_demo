from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, trim, concat, concat_ws, substring, 
    regexp_replace, when, length, current_timestamp, 
    to_date, to_timestamp, date_format, coalesce, expr
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def apply_address_change_transformations(
    source_df: DataFrame, 
    yuyu_clnt_df: DataFrame, 
    yuyuk_cln_df: DataFrame,
    process_userid: str
) -> tuple[DataFrame, DataFrame]:
    """
    Apply all transformations needed for the address change YUYU process
    
    Args:
        source_df: Source DataFrame with address change data
        yuyu_clnt_df: Lookup table for policy owner name components
        yuyuk_cln_df: Lookup table for policy owner Kanji name
        process_userid: User ID to stamp in process records
        
    Returns:
        tuple: (yuyu_output_df, staging_update_df) - DataFrames for the output file and staging table updates
    """
    
    # Step 1: Initial pass-through and preparation (EXP_ADD_CHNG equivalent)
    exp_add_chng_df = source_df
    
    # Step 2: Join with lookup tables for owner name enrichment
    # LKP_YUYU_CLNT lookup - policy owner name components
    enriched_df = exp_add_chng_df.join(
        yuyu_clnt_df,
        exp_add_chng_df["T_TX_BASIC_POLICY_ID"] == yuyu_clnt_df["POL_NO"],
        "left"
    )
    
    # LKP_YUYUK_CLN lookup - policy owner Kanji name
    enriched_df = enriched_df.join(
        yuyuk_cln_df,
        enriched_df["T_TX_BASIC_POLICY_ID"] == yuyuk_cln_df["POL_NO"],
        "left"
    )
    
    # Step 3: Apply business transformations (EXP_YUYU_CHNG equivalent)
    transformed_df = enriched_df.withColumn(
        # BR-03: Reception date formatting and conversion
        "O__REQUEST_ACCEPT_DATETIME", 
        to_timestamp(
            date_format(col("T_TX_REQUEST_REQ_ACC_DATETIME"), "yyyy/MM/dd HH:mm"), 
            "yyyy-MM-dd HH:mm"
        )
    ).withColumn(
        # BR-05: Postal code fallback and normalization
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
        # BR-04: New address (Kana) fallback to PPAY_* when ZIP is missing
        "zip_missing",
        (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
        (trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == "") | 
        (length(trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))) == 0)
    ).withColumn(
        "o_T_TX_DTL_ADD_CH_N_TRANS_ADD1",
        when(col("zip_missing"), col("PPAY_ADR1_FW"))
        .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD1"))
    ).withColumn(
        "o_T_TX_DTL_ADD_CH_N_TRANS_ADD2",
        when(col("zip_missing"), col("PPAY_ADR2_FW"))
        .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD2"))
    ).withColumn(
        "o_T_TX_DTL_ADD_CH_N_TRANS_ADD3",
        when(col("zip_missing"), col("PPAY_ADR3_FW"))
        .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD3"))
    ).withColumn(
        # BR-06: Telephone number formatting
        "phone_trimmed",
        trim(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"))
    ).withColumn(
        "phone_prefix",
        substring(col("phone_trimmed"), 1, 3)
    ).withColumn(
        "O_NEW_PHONE_NUMBER",
        when(
            (col("phone_trimmed").isNull()) | (col("phone_trimmed") == ""),
            lit("")
        ).when(
            col("phone_prefix").isin("050", "060", "070", "080", "090"),
            concat(
                substring(col("phone_trimmed"), 1, 3), 
                lit("-"), 
                substring(col("phone_trimmed"), 4, 4),
                lit("-"),
                substring(col("phone_trimmed"), 8, 6)
            )
        ).otherwise(
            concat(
                substring(col("phone_trimmed"), 1, 2), 
                lit("-"), 
                substring(col("phone_trimmed"), 3, 4),
                lit("-"),
                substring(col("phone_trimmed"), 7, 6)
            )
        )
    ).withColumn(
        # BR-07: Policy owner name derivation (Kana)
        "O_KANA_NAME",
        when(
            (col("POWN_FNM").isNull()) | (trim(col("POWN_FNM")) == ""),
            trim(col("POWN_LNM"))
        ).otherwise(
            concat_ws(" ", trim(col("POWN_LNM")), trim(col("POWN_FNM")))
        )
    ).withColumn(
        # BR-01: Stamp processing date on staging updates
        "o_STG_PROCESS_DATE",
        current_timestamp()
    ).withColumn(
        # BR-02: Stamp processing user on staging updates
        "o_STG_PROCESS_USERID",
        lit(process_userid)
    )
    
    # Step 4: Generate sequence numbers (SEQTRANS equivalent)
    window_spec = Window.orderBy("T_TX_REQUEST_REQUEST_ID")
    sequence_df = transformed_df.withColumn("SEQUENCE_NUMBER", F.row_number().over(window_spec) - 1)
    
    # Step 5: Create output DataFrame for DPAddressChangeYUYU
    yuyu_output_df = sequence_df.select(
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
    
    # Step 6: Create update DataFrame for STG_E2E_AC_TXDBH_DATA_TGT
    staging_update_df = sequence_df.select(
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
    
    return yuyu_output_df, staging_update_df