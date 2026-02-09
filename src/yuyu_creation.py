"""
YUYU file creation module for E2E Address Change YUYU Creation workflow.
Replaces the Informatica session s_E2E_AC_TXDBH_DP_YUYU_CREATION.
"""
import os
import logging
from datetime import datetime
from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, expr, when, trim, concat, concat_ws, 
    regexp_replace, substring, to_date, current_timestamp
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

def process_yuyu_records(
    spark: SparkSession,
    db_connection_stg: str,
    db_connection_ods: str,
    output_file_path: str,
    target_file_dir: str,
    file_timestamp: str
) -> None:
    """
    Process YUYU records to create the outbound file and update staging table.
    
    Args:
        spark: SparkSession object
        db_connection_stg: Oracle staging database connection string
        db_connection_ods: Oracle ODS database connection string
        output_file_path: Path for the output YUYU file
        target_file_dir: Directory for target files
        file_timestamp: Timestamp for this file run
    """
    try:
        logger.info("Starting YUYU record processing")
        
        # Step 1: Extract source data
        source_df = extract_source_data(spark, db_connection_stg)
        
        # Step 2: Perform lookups to get policy owner information
        enriched_df = perform_lookups(spark, source_df, db_connection_ods)
        
        # Step 3: Apply transformations
        transformed_df = apply_transformations(enriched_df)
        
        # Step 4: Generate sequence numbers
        final_df = add_sequence_numbers(transformed_df)
        
        # Step 5: Write output file
        write_output_file(final_df, output_file_path)
        
        # Step 6: Update staging table with processing status
        update_staging_table(spark, db_connection_stg, final_df)
        
        logger.info("YUYU record processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing YUYU records: {str(e)}", exc_info=True)
        raise

def extract_source_data(spark: SparkSession, db_connection: str) -> DataFrame:
    """
    Extract source data from staging table with PPAY normalization.
    
    Args:
        spark: SparkSession object
        db_connection: Oracle staging database connection string
    
    Returns:
        DataFrame: Source data with normalized PPAY fields
    """
    logger.info("Extracting source data")
    
    # Define the SQL query with PPAY normalization
    # This simulates the SQL override in the Informatica Source Qualifier
    sql_query = """
    SELECT 
        T_TX_REQUEST_REQUEST_ID,
        T_TX_BASIC_TRANS_ID,
        T_TX_RELATION_TRANS_REL_ID,
        T_TX_REQ_POL_REQUEST_POLICY_ID,
        STG_POLICY_ID,
        T_TX_REQUEST_ORIGIN_REQUEST_ID,
        T_TX_BASIC_POLICY_ID,
        T_TX_REQUEST_REQ_ACC_DATETIME,
        T_TX_DTL_ADD_CH_N_TRANS_ZIP,
        T_TX_DTL_ADD_CH_N_TRANS_ADD1,
        T_TX_DTL_ADD_CH_N_TRANS_ADD2,
        T_TX_DTL_ADD_CH_N_TRANS_ADD3,
        T_TX_DTL_ADD_CH_N_TRANS_AD1_KJ,
        T_TX_DTL_ADD_CH_N_TRANS_AD2_KJ,
        T_TX_DTL_ADD_CH_N_TRANS_AD3_KJ,
        T_TX_DTL_ADD_CH_N_TRANS_PHNO,
        STG_TXDB_STATUS,
        -- PPAY normalized fields would be derived in Oracle using functions like
        -- to_single_byte, to_multi_byte, utl_i18n.transliterate, etc.
        -- For PySpark, we'll simulate these as direct columns for now
        PPAY_ZIP,
        PPAY_ADR1_FW,
        PPAY_ADR2_FW,
        PPAY_ADR3_FW
    FROM 
        ZSYSE2EDEV.STG_E2E_AC_TXDBH_DATA
    """
    
    # In a real implementation, we'd use a JDBC connection with the SQL query
    # For now, we'll simulate the extraction with a direct table read
    source_df = spark.read \
        .format("jdbc") \
        .option("url", db_connection) \
        .option("query", sql_query) \
        .option("user", os.environ.get("DB_USER")) \
        .option("password", os.environ.get("DB_PASSWORD")) \
        .load()
    
    return source_df

def perform_lookups(spark: SparkSession, source_df: DataFrame, db_connection_ods: str) -> DataFrame:
    """
    Perform lookups to get policy owner information.
    
    Args:
        spark: SparkSession object
        source_df: Source data DataFrame
        db_connection_ods: Oracle ODS database connection string
    
    Returns:
        DataFrame: Source data enriched with lookup information
    """
    logger.info("Performing lookups for policy owner information")
    
    # Load lookup tables
    lookup_yuyu_clnt = spark.read \
        .format("jdbc") \
        .option("url", db_connection_ods) \
        .option("dbtable", f"{os.environ.get('PARAM_LKPSCHEMA')}.{os.environ.get('PARAM_LKPTBL_1')}") \
        .option("user", os.environ.get("DB_USER")) \
        .option("password", os.environ.get("DB_PASSWORD")) \
        .load() \
        .select("POL_NO", "POWN_LNM", "POWN_FNM")
    
    lookup_yuyuk_cln = spark.read \
        .format("jdbc") \
        .option("url", db_connection_ods) \
        .option("dbtable", f"{os.environ.get('PARAM_LKPSCHEMA')}.{os.environ.get('PARAM_LKPTBL_2')}") \
        .option("user", os.environ.get("DB_USER")) \
        .option("password", os.environ.get("DB_PASSWORD")) \
        .load() \
        .select("POL_NO", "POWN_KNM")
    
    # Join with first lookup for Kana name components
    df_with_kana = source_df.join(
        lookup_yuyu_clnt,
        source_df["T_TX_BASIC_POLICY_ID"] == lookup_yuyu_clnt["POL_NO"],
        "left"
    )
    
    # Join with second lookup for Kanji name
    enriched_df = df_with_kana.join(
        lookup_yuyuk_cln,
        df_with_kana["T_TX_BASIC_POLICY_ID"] == lookup_yuyuk_cln["POL_NO"],
        "left"
    )
    
    return enriched_df

def apply_transformations(df: DataFrame) -> DataFrame:
    """
    Apply business transformations to the data.
    
    Args:
        df: Input DataFrame with source and lookup data
    
    Returns:
        DataFrame: Transformed data ready for output
    """
    logger.info("Applying business transformations")
    
    # Apply all transformations from EXP_YUYU_CHNG
    transformed_df = df \
        .withColumn(
            # BR-02: Address fallback behavior (PPAY substitution)
            "v_T_TX_DTL_ADD_CH_N_TRANS_ZIP", 
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP") == "") | 
                (length(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == 0),
                col("PPAY_ZIP")
            ).otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP"))
        ) \
        .withColumn(
            # BR-03: Postal code normalization
            "NEW_ADDRESS_POSTAL_CODE",
            regexp_replace(col("v_T_TX_DTL_ADD_CH_N_TRANS_ZIP"), "-", "")
        ) \
        .withColumn(
            # BR-02: Address line 1 fallback
            "NEW_ADDRESS_KANA_1",
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP") == "") | 
                (length(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == 0),
                col("PPAY_ADR1_FW")
            ).otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD1"))
        ) \
        .withColumn(
            # BR-02: Address line 2 fallback
            "NEW_ADDRESS_KANA_2",
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP") == "") | 
                (length(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == 0),
                col("PPAY_ADR2_FW")
            ).otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD2"))
        ) \
        .withColumn(
            # BR-02: Address line 3 fallback
            "NEW_ADDRESS_KANA_3",
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP").isNull()) | 
                (col("T_TX_DTL_ADD_CH_N_TRANS_ZIP") == "") | 
                (length(col("T_TX_DTL_ADD_CH_N_TRANS_ZIP")) == 0),
                col("PPAY_ADR3_FW")
            ).otherwise(col("T_TX_DTL_ADD_CH_N_TRANS_ADD3"))
        ) \
        .withColumn(
            # Kanji address fields pass-through
            "NEW_ADDRESS_KANJI_1", col("T_TX_DTL_ADD_CH_N_TRANS_AD1_KJ")
        ) \
        .withColumn(
            "NEW_ADDRESS_KANJI_2", col("T_TX_DTL_ADD_CH_N_TRANS_AD2_KJ")
        ) \
        .withColumn(
            "NEW_ADDRESS_KANJI_3", col("T_TX_DTL_ADD_CH_N_TRANS_AD3_KJ")
        ) \
        .withColumn(
            # BR-04: Telephone normalization
            "NEW_ADDRESS_TELEPHONE_NUMBER",
            when(
                (col("T_TX_DTL_ADD_CH_N_TRANS_PHNO").isNull()) | 
                (col("T_TX_DTL_ADD_CH_N_TRANS_PHNO") == ""),
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
        ) \
        .withColumn(
            # BR-05: Policy owner Kana name concatenation
            "POLICY_OWNER_NAME_KANA",
            when(
                (col("POWN_FNM").isNull()) | (trim(col("POWN_FNM")) == ""),
                trim(col("POWN_LNM"))
            ).otherwise(
                concat_ws(" ", trim(col("POWN_LNM")), trim(col("POWN_FNM")))
            )
        ) \
        .withColumn(
            # Policy owner Kanji name pass-through
            "POLICY_OWNER_NAME_KANJI", col("POWN_KNM")
        ) \
        .withColumn(
            # Reception date formatting
            # Note: Fixing the format mismatch issue mentioned in the requirements
            "RECEPTION_DATE",
            to_date(
                date_format(col("T_TX_REQUEST_REQ_ACC_DATETIME"), "yyyy-MM-dd HH:mm"),
                "yyyy-MM-dd HH:mm"
            )
        ) \
        .withColumn(
            # BR-06: Staging update audit - Process date
            "STG_PROCESS_DATE", current_timestamp()
        ) \
        .withColumn(
            # BR-06: Staging update audit - Process user ID
            "STG_PROCESS_USERID", lit(os.environ.get("PROCESS_USERID", "PYSPARK"))
        ) \
        .withColumn(
            # Pass through required fields
            "ORIGIN_REQUEST_ID", col("T_TX_REQUEST_ORIGIN_REQUEST_ID")
        ) \
        .withColumn(
            "POLICY_ID", col("T_TX_BASIC_POLICY_ID")
        )
    
    return transformed_df

def add_sequence_numbers(df: DataFrame) -> DataFrame:
    """
    Add sequence numbers to the DataFrame.
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame: DataFrame with sequence numbers added
    """
    logger.info("Adding sequence numbers")
    
    # Create a window spec with no partitioning to generate sequential numbers
    window_spec = Window.orderBy(lit(1))
    
    # Add sequence numbers starting from 0
    final_df = df.withColumn("SEQUENCE_NUMBER", F.row_number().over(window_spec) - 1)
    
    return final_df

def write_output_file(df: DataFrame, output_file_path: str) -> None:
    """
    Write the output file in the required format.
    
    Args:
        df: DataFrame to write
        output_file_path: Path for the output file
    """
    logger.info(f"Writing output file to {output_file_path}")
    
    # Select only the columns needed for the output file in the correct order
    output_columns = [
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
    ]
    
    # Write the file with no header
    df.select(output_columns) \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "false") \
        .option("delimiter", ",") \
        .csv(output_file_path)

def update_staging_table(spark: SparkSession, db_connection: str, df: DataFrame) -> None:
    """
    Update the staging table with processing status and audit information.
    
    Args:
        spark: SparkSession object
        db_connection: Oracle staging database connection string
        df: DataFrame with processed data
    """
    logger.info("Updating staging table")
    
    # Select only the columns needed for the update
    update_columns = [
        "T_TX_REQUEST_REQUEST_ID",
        "T_TX_BASIC_TRANS_ID",
        "T_TX_RELATION_TRANS_REL_ID",
        "T_TX_REQ_POL_REQUEST_POLICY_ID",
        "STG_POLICY_ID",
        "STG_TXDB_STATUS",
        "STG_PROCESS_DATE",
        "STG_PROCESS_USERID"
    ]
    
    update_df = df.select(update_columns)
    
    # In a real implementation, we would use JDBC batch updates
    # For now, we'll simulate the update by writing to a temporary table
    # and then executing an update statement
    
    # Write to a temporary table
    temp_table_name = f"{os.environ.get('PARAM_TGTSCHEMA')}.TEMP_STG_UPDATE"
    
    update_df.write \
        .format("jdbc") \
        .option("url", db_connection) \
        .option("dbtable", temp_table_name) \
        .option("user", os.environ.get("DB_USER")) \
        .option("password", os.environ.get("DB_PASSWORD")) \
        .mode("overwrite") \
        .save()
    
    # Execute update statement
    # In a real implementation, we would use JDBC to execute this SQL
    update_sql = f"""
    UPDATE {os.environ.get('PARAM_TGTSCHEMA')}.{os.environ.get('PARAM_TGTTABLE_1')} tgt
    SET 
        tgt.STG_TXDB_STATUS = tmp.STG_TXDB_STATUS,
        tgt.STG_PROCESS_DATE = tmp.STG_PROCESS_DATE,
        tgt.STG_PROCESS_USERID = tmp.STG_PROCESS_USERID
    FROM {temp_table_name} tmp
    WHERE 
        tgt.T_TX_REQUEST_REQUEST_ID = tmp.T_TX_REQUEST_REQUEST_ID
        AND tgt.T_TX_BASIC_TRANS_ID = tmp.T_TX_BASIC_TRANS_ID
        AND tgt.T_TX_RELATION_TRANS_REL_ID = tmp.T_TX_RELATION_TRANS_REL_ID
        AND tgt.T_TX_REQ_POL_REQUEST_POLICY_ID = tmp.T_TX_REQ_POL_REQUEST_POLICY_ID
        AND tgt.STG_POLICY_ID = tmp.STG_POLICY_ID
    """
    
    # In a real implementation, we would execute this SQL
    # For now, we'll just log it
    logger.info(f"Would execute: {update_sql}")
    
    # Clean up temporary table
    spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")