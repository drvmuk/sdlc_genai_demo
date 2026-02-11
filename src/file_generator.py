"""
File generator module for creating 10K flat files with proper naming and partitioning.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, concat, monotonically_increasing_id
import os
from datetime import datetime

def calculate_filename(df: DataFrame, config: dict) -> DataFrame:
    """
    Calculate file name for each record based on configuration.
    Implements EXP_Calc_10K_FileName functionality.
    
    Args:
        df (DataFrame): Input DataFrame
        config (dict): Configuration parameters
        
    Returns:
        DataFrame: DataFrame with added File_Path_Name column
    """
    # Get current date for filename
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Create a base filename pattern
    base_filename = f"{config['file_prefix']}_{current_date}"
    
    # Add batch_id if provided
    if config['batch_id']:
        base_filename += f"_{config['batch_id']}"
    
    # Add run_id if provided
    if config['run_id']:
        base_filename += f"_{config['run_id']}"
    
    # Add a row number column for partitioning
    df_with_row_num = df.withColumn("row_num", monotonically_increasing_id())
    
    # Calculate file sequence number (1-based) for each row
    # Each file will contain max_rows_per_file records
    df_with_file_seq = df_with_row_num.withColumn(
        "file_seq", 
        (col("row_num") / config['max_rows_per_file']).cast("int") + 1
    )
    
    # Create the full file path for each row
    df_with_filename = df_with_file_seq.withColumn(
        "File_Path_Name",
        concat(
            lit(os.path.join(config['output_dir'], base_filename)),
            lit("_"),
            col("file_seq").cast("string"),
            lit("."),
            lit(config['file_ext'])
        )
    )
    
    # Drop the temporary columns
    result_df = df_with_filename.drop("row_num", "file_seq")
    
    return result_df

def write_10k_files(df: DataFrame, config: dict) -> None:
    """
    Write data to 10K flat files using transaction control.
    Implements TC_Seperate_10K_Files functionality.
    
    Args:
        df (DataFrame): Input DataFrame with File_Path_Name column
        config (dict): Configuration parameters
    """
    # Ensure output directory exists
    os.makedirs(config['output_dir'], exist_ok=True)
    
    # Repartition the DataFrame based on File_Path_Name to ensure each file is written separately
    partitioned_df = df.repartition("File_Path_Name")
    
    # Write the files
    # The format is "csv" but we'll use a custom delimiter to simulate a flat file
    partitioned_df.write \
        .format("csv") \
        .option("header", "false") \
        .option("delimiter", "|") \
        .option("encoding", "UTF-8") \
        .option("quote", "\u0000") \
        .mode("overwrite") \
        .partitionBy("File_Path_Name") \
        .save(f"{config['output_dir']}/temp")
    
    # Now rename the files to their actual names
    # This is a simplified approach - in a production environment,
    # you might want to use a more robust method to handle this
    import subprocess
    import glob
    
    # Get all part files
    part_files = glob.glob(f"{config['output_dir']}/temp/File_Path_Name=*/*.csv")
    
    for part_file in part_files:
        # Extract the target filename from the partition directory
        dir_name = os.path.dirname(part_file)
        file_path_name = dir_name.split("File_Path_Name=")[1].replace("%2F", "/")
        
        # Create target directory if it doesn't exist
        target_dir = os.path.dirname(file_path_name)
        os.makedirs(target_dir, exist_ok=True)
        
        # Move the file to its final location
        subprocess.run(["mv", part_file, file_path_name])
    
    # Clean up temporary directory
    subprocess.run(["rm", "-rf", f"{config['output_dir']}/temp"])