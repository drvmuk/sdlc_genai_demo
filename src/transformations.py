from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, expr, when, lit, trim, concat, concat_ws, substring, 
    length, regexp_replace, to_date, to_timestamp, current_timestamp,
    isnull, coalesce, monotonically_increasing_id
)


def check_source_record_count(df: DataFrame) -> int:
    """
    Count source records and return the count.
    
    Args:
        df: Source DataFrame
        
    Returns:
        int: Count of source records
    """
    return df.count()


def format_reception_date(df: DataFrame) -> DataFrame:
    """
    Format reception date according to requirements.
    
    Args:
        df: DataFrame with reception date column
        
    Returns:
        DataFrame: DataFrame with formatted reception date
    """
    return df.withColumn(
        "RECEPTION_DATE",
        to_date(
            to_timestamp(
                col("T_TX_REQUEST_REQ_ACC_DATETIME"),
                "yyyy/MM/dd HH:mm"
            ),
            "yyyy-MM-dd HH:mm"
        )
    )


def normalize_postal_code(df: DataFrame) -> DataFrame:
    """
    Normalize postal code by removing hyphens and applying fallback logic.
    
    Args:
        df: DataFrame with postal code columns
        
    Returns:
        DataFrame: DataFrame with normalized postal code
    """
    return df.withColumn(
        "NEW_ADDRESS_POSTAL_CODE",
        when(
            (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
            (trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == "") | 
            (length(trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))) == 0),
            regexp_replace(col("PPAY_ZIP"), "-", "")
        ).otherwise(
            regexp_replace(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"), "-", "")
        )
    )


def apply_address_fallback(df: DataFrame) -> DataFrame:
    """
    Apply address fallback logic for address fields.
    
    Args:
        df: DataFrame with address columns
        
    Returns:
        DataFrame: DataFrame with fallback logic applied
    """
    # Check if zip is missing to determine whether to use fallback
    zip_missing = (
        (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
        (trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == "") | 
        (length(trim(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))) == 0)
    )
    
    return df \
        .withColumn(
            "NEW_ADDRESS_KANA_1",
            when(zip_missing, col("PPAY_ADR1_FW"))
            .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD1"))
        ) \
        .withColumn(
            "NEW_ADDRESS_KANA_2",
            when(zip_missing, col("PPAY_ADR2_FW"))
            .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD2"))
        ) \
        .withColumn(
            "NEW_ADDRESS_KANA_3",
            when(zip_missing, col("PPAY_ADR3_FW"))
            .otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD3"))
        ) \
        .withColumn(
            "NEW_ADDRESS_KANJI_1",
            col("T_TX_DTL_ADD_CH_N_TRANS_AD1_KJ")
        ) \
        .withColumn(
            "NEW_ADDRESS_KANJI_2",
            col("T_TX_DTL_ADD_CH_N_TRANS_AD2_KJ")
        ) \
        .withColumn(
            "NEW_ADDRESS_KANJI_3",
            col("T_TX_DTL_ADD_CH_N_TRANS_AD3_KJ")
        )


def format_telephone_number(df: DataFrame) -> DataFrame:
    """
    Format telephone number according to requirements.
    
    Args:
        df: DataFrame with phone number column
        
    Returns:
        DataFrame: DataFrame with formatted phone number
    """
    return df.withColumn(
        "NEW_ADDRESS_TELEPHONE_NUMBER",
        when(
            (col("T_TX_DTL_ADD_CH_N_TRANS_PHNO").isNull()) |
            (trim(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO")) == ""),
            lit("")
        ).otherwise(
            when(
                substring(col("T_TX_DTL_ADD_CH_N_TRANS_PHNO"), 1, 3).isin(
                    "050", "060", "070", "080", "090"
                ),
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
        )
    )


def format_policy_owner_name(df: DataFrame) -> DataFrame:
    """
    Format policy owner name from lookup data.
    
    Args:
        df: DataFrame with policy owner name columns
        
    Returns:
        DataFrame: DataFrame with formatted policy owner names
    """
    return df.withColumn(
        "POLICY_OWNER_NAME_KANA",
        when(
            (col("POWN_FNM").isNull()) | (trim(col("POWN_FNM")) == ""),
            trim(col("POWN_LNM"))
        ).otherwise(
            concat_ws(" ", trim(col("POWN_LNM")), trim(col("POWN_FNM")))
        )
    ).withColumn(
        "POLICY_OWNER_NAME_KANJI",
        col("POWN_KNM")
    )


def add_sequence_number(df: DataFrame) -> DataFrame:
    """
    Add sequence number to DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with sequence number column
    """
    # Use monotonically_increasing_id to generate sequence numbers
    # In production, this would be replaced with a proper sequence generator
    return df.withColumn("SEQUENCE_NUMBER", monotonically_increasing_id())


def prepare_staging_update(df: DataFrame, process_userid: str) -> DataFrame:
    """
    Prepare DataFrame for staging table update with audit fields.
    
    Args:
        df: Input DataFrame
        process_userid: User ID for audit
        
    Returns:
        DataFrame: DataFrame with audit columns for staging update
    """
    return df.withColumn(
        "STG_PROCESS_DATE", 
        current_timestamp()
    ).withColumn(
        "STG_PROCESS_USERID", 
        lit(process_userid)
    )


def select_output_columns(df: DataFrame) -> DataFrame:
    """
    Select columns for the output file.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with only output columns
    """
    return df.select(
        "SEQUENCE_NUMBER",
        "ORIGIN_REQUEST_ID",
        "POLICY_ID",
        "RECEPTION_DATE",
        "NEW_ADDRESS_POSTAL_CODE",
        "NEW_ADDRESS_KANA_1",
        "NEW_ADDRESS_KANJI_1",
        "NEW_ADDRESS_KANA_2",
        "NEW_ADDRESS_KANJI_2",
        "NEW_ADDRESS_KANA_3",
        "NEW_ADDRESS_KANJI_3",
        "NEW_ADDRESS_TELEPHONE_NUMBER",
        "POLICY_OWNER_NAME_KANA",
        "POLICY_OWNER_NAME_KANJI"
    )


def select_staging_update_columns(df: DataFrame) -> DataFrame:
    """
    Select columns for staging table update.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with columns for staging update
    """
    return df.select(
        # Key columns for update
        "T_TX_REQUEST_REQUEST_ID",
        "T_TX_BASIC_TRANS_ID",
        "T_TX_RELATION_TRANS_REL_ID",
        "T_TX_REQ_POL_REQUEST_POLICY_ID",
        "STG_POLICY_ID",
        # Update columns
        "STG_TXDB_STATUS",
        "STG_PROCESS_DATE",
        "STG_PROCESS_USERID"
    )