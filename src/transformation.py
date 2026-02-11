"""
Data transformation module for E2E Policy Services.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, concat, 
    trim, nvl, coalesce, expr, monotonically_increasing_id
)
from typing import Dict
from .config import DEFAULT_VALUES

def transform_policy_data(source_tables: Dict[str, DataFrame]) -> DataFrame:
    """
    Transform and join the source tables to create the target dataset.
    
    Args:
        source_tables: Dictionary of source table DataFrames
        
    Returns:
        DataFrame: The transformed data ready for loading
    """
    # Extract source tables
    tx_request_policy = source_tables["T_TX_REQUEST_POLICY"].alias("trp")
    tx_basic = source_tables["T_TX_BASIC"].alias("ttb")
    tx_relation = source_tables["T_TX_RELATION"].alias("ttr")
    tx_request = source_tables["T_TX_REQUEST"].alias("tr")
    tx_dtl_beneficiary_change = source_tables["T_TX_DTL_BENEFICIARY_CHANGE"].alias("ttdbc")
    
    # Join tx_request_policy and tx_basic
    joined_df = tx_request_policy.join(
        tx_basic,
        (tx_request_policy["POLICY_ID"] == tx_basic["POLICY_ID"]) &
        (tx_request_policy["REQUEST_ID"] == tx_basic["REQUEST_ID"]),
        "inner"
    )
    
    # Join with tx_request
    joined_df = joined_df.join(
        tx_request,
        joined_df["REQUEST_ID"] == tx_request["REQUEST_ID"],
        "inner"
    )
    
    # Join with tx_relation
    joined_df = joined_df.join(
        tx_relation,
        (joined_df["TRANSACTION_ID"] == tx_relation["TRANSACTION_ID"]) &
        (tx_relation["OBJECT_TYPE"] == "BENEFICIARY"),
        "left"
    )
    
    # Join with tx_dtl_beneficiary_change
    joined_df = joined_df.join(
        tx_dtl_beneficiary_change,
        joined_df["TRANSACTION_ID"] == tx_dtl_beneficiary_change["TRANSACTION_ID"],
        "left"
    )
    
    # Apply transformations to create target columns
    result_df = joined_df.select(
        # FNL_POLICY_ID
        col("trp.POLICY_ID").alias("FNL_POLICY_ID"),
        
        # TR_REQUEST_ID
        col("tr.REQUEST_ID").alias("TR_REQUEST_ID"),
        
        # TR_REQUEST_STATUS
        col("tr.REQUEST_STATUS").alias("TR_REQUEST_STATUS"),
        
        # TR_UPDATE_DATET
        col("tr.UPDATE_DATET").alias("TR_UPDATE_DATET"),
        
        # TTB_REQUEST_ID
        col("ttb.REQUEST_ID").alias("TTB_REQUEST_ID"),
        
        # TTB_POLICY_ID
        col("ttb.POLICY_ID").alias("TTB_POLICY_ID"),
        
        # TTB_TRANSACTION_ID
        col("ttb.TRANSACTION_ID").alias("TTB_TRANSACTION_ID"),
        
        # TTB_TRANSACTION_TYPE
        col("ttb.TRANSACTION_TYPE").alias("TTB_TRANSACTION_TYPE"),
        
        # TTB_DISCARD_FLAG
        col("ttb.DISCARD_FLAG").alias("TTB_DISCARD_FLAG"),
        
        # TTB_TRANSACTION_STATUS
        col("ttb.TRANSACTION_STATUS").alias("TTB_TRANSACTION_STATUS"),
        
        # TRP_TRANSACTION_ID
        col("trp.TRANSACTION_ID").alias("TRP_TRANSACTION_ID"),
        
        # TRP_POLICY_ID
        col("trp.POLICY_ID").alias("TRP_POLICY_ID"),
        
        # TRP_OTHER_POLICY_ID
        col("trp.OTHER_POLICY_ID").alias("TRP_OTHER_POLICY_ID"),
        
        # TRP_SRC_SYS_MSTR_ID
        col("trp.SRC_SYS_MSTR_ID").alias("TRP_SRC_SYS_MSTR_ID"),
        
        # TRP_OTHER_POLICY_TYPE
        col("trp.OTHER_POLICY_TYPE").alias("TRP_OTHER_POLICY_TYPE"),
        
        # TRP_POWN_NAME_NUMBER
        col("trp.POWN_NAME_NUMBER").alias("TRP_POWN_NAME_NUMBER"),
        
        # TTR_TRANSACTION_ID
        col("ttr.TRANSACTION_ID").alias("TTR_TRANSACTION_ID"),
        
        # TTR_BENEFICIARY_TYPE
        col("ttr.BENEFICIARY_TYPE").alias("TTR_BENEFICIARY_TYPE"),
        
        # TTR_OBJECT_SUB_TYPE
        col("ttr.OBJECT_SUB_TYPE").alias("TTR_OBJECT_SUB_TYPE"),
        
        # TTR_TRANSACTION_RELATION_ID
        col("ttr.TRANSACTION_RELATION_ID").alias("TTR_TRANSACTION_RELATION_ID"),
        
        # TTR_NAME_NUMBER
        col("ttr.NAME_NUMBER").alias("TTR_NAME_NUMBER"),
        
        # TTDBC_REQUEST_ID
        col("ttdbc.REQUEST_ID").alias("TTDBC_REQUEST_ID"),
        
        # TTDBC_TRANSACTION_ID
        col("ttdbc.TRANSACTION_ID").alias("TTDBC_TRANSACTION_ID"),
        
        # TTDBC_CHANGE_STATE
        col("ttdbc.CHANGE_STATE").alias("TTDBC_CHANGE_STATE"),
        
        # TTDBC_LASTNAME_KANA
        col("ttdbc.LASTNAME_KANA").alias("TTDBC_LASTNAME_KANA"),
        
        # TTDBC_FIRSTNAME_KANA
        col("ttdbc.FIRSTNAME_KANA").alias("TTDBC_FIRSTNAME_KANA"),
        
        # TTDBC_GENDER_CODE
        col("ttdbc.GENDER_CODE").alias("TTDBC_GENDER_CODE"),
        
        # TTDBC_LASTNAME
        col("ttdbc.LASTNAME").alias("TTDBC_LASTNAME"),
        
        # TTDBC_FIRSTNAME
        col("ttdbc.FIRSTNAME").alias("TTDBC_FIRSTNAME"),
        
        # TTDBC_PERCENTAGE
        col("ttdbc.PERCENTAGE").alias("TTDBC_PERCENTAGE"),
        
        # TTDBC_EFFECTIVE_DATE
        col("ttdbc.EFFECTIVE_DATE").alias("TTDBC_EFFECTIVE_DATE"),
        
        # DATA_TYPE
        lit(DEFAULT_VALUES["DATA_TYPE"]).alias("DATA_TYPE"),
        
        # POLICY_TYPE
        lit(DEFAULT_VALUES["POLICY_TYPE"]).alias("POLICY_TYPE"),
        
        # MC_CRNCY
        lit(DEFAULT_VALUES["MC_CRNCY"]).alias("MC_CRNCY"),
        
        # CREATE_DATETIME
        current_timestamp().alias("CREATE_DATETIME"),
        
        # TTDBC_CHANGE_ID - Generate a unique ID
        monotonically_increasing_id().alias("TTDBC_CHANGE_ID")
    )
    
    # Filter out any records with null policy IDs
    result_df = result_df.filter(col("FNL_POLICY_ID").isNotNull())
    
    return result_df