from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

def apply_scd_type_2(df: DataFrame, id_column: str, update_columns: list) -> DataFrame:
    # Existing records
    existing_df = df.filter(col("IsActive") == lit(True))
    
    # New/updated records
    new_df = df.filter(col("IsActive") == lit(None))
    
    # Identify updated records
    updated_df = existing_df.join(new_df, on=id_column, how="inner")
    updated_df = updated_df.filter(updated_df["hash"] != updated_df["new_hash"])
    
    # Deactivate existing records that have been updated
    deactivated_df = updated_df.select(
        col(id_column),
        lit(False).alias("IsActive"),
        col("CreateDateTime"),
        current_timestamp().alias("UpdateDateTime")
    )
    
    # Activate new/updated records
    activated_df = new_df.select(
        col(id_column),
        lit(True).alias("IsActive"),
        current_timestamp().alias("CreateDateTime"),
        current_timestamp().alias("UpdateDateTime")
    )
    
    # Combine deactivated and activated records
    result_df = existing_df.unionByName(deactivated_df).unionByName(activated_df)
    
    return result_df