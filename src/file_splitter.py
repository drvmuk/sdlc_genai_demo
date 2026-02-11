"""
Module for splitting files to maintain maximum records per file.
"""
import logging
import os
from typing import List, Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import monotonically_increasing_id, lit, col, floor, div

from src.field_calculator import generate_file_name

logger = logging.getLogger(__name__)


def split_dataframe_by_row_count(
    spark: SparkSession,
    df: DataFrame, 
    output_dir: str,
    file_prefix: str,
    batch_id: str,
    file_ext: str,
    max_rows_per_file: int
) -> List[Dict[str, Any]]:
    """
    Split a DataFrame into multiple files based on maximum rows per file.
    
    Args:
        spark: SparkSession
        df: Input DataFrame
        output_dir: Output directory
        file_prefix: File prefix
        batch_id: Batch ID
        file_ext: File extension
        max_rows_per_file: Maximum rows per file
        
    Returns:
        List of dictionaries with file information
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Add row number for partitioning
    df_with_id = df.withColumn("row_id", monotonically_increasing_id())
    
    # Calculate file number for each row
    df_with_file_num = df_with_id.withColumn(
        "file_num", 
        floor(col("row_id") / max_rows_per_file) + 1
    )
    
    # Get distinct file numbers
    file_nums = df_with_file_num.select("file_num").distinct().collect()
    file_info = []
    
    # Process each file partition
    for row in file_nums:
        file_num = row["file_num"]
        
        # Filter rows for this file
        file_df = df_with_file_num.filter(col("file_num") == file_num)
        
        # Generate file name
        file_df_with_name = generate_file_name(
            file_df, 
            output_dir, 
            file_prefix, 
            batch_id, 
            file_ext, 
            int(file_num)
        )
        
        # Get file name for reporting
        file_name = file_df_with_name.select("FileName").first()["FileName"]
        
        # Select only the required columns for output (remove row_id and file_num)
        output_columns = [c for c in file_df_with_name.columns 
                         if c not in ["row_id", "file_num"]]
        output_df = file_df_with_name.select(*output_columns)
        
        # Write to file
        output_df.write.mode("overwrite").option("header", "false").csv(
            file_name, 
            sep="|"  # Using pipe as delimiter, adjust as needed
        )
        
        # Collect file info for reporting
        row_count = output_df.count()
        file_info.append({
            "file_name": file_name,
            "row_count": row_count
        })
        
        logger.info(f"Generated file {file_name} with {row_count} rows")
    
    return file_info