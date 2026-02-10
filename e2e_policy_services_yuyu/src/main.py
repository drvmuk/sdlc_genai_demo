"""
Main entry point for E2E Policy Services YUYU extract pipeline.
"""

from pyspark.sql import SparkSession
from config import Config
from data_loader import DataLoader
from transformations import YUYUTransformations
from data_writer import DataWriter


def create_spark_session() -> SparkSession:
    """Create and configure Spark session."""
    config = Config()
    
    builder = SparkSession.builder.appName("E2E_YUYU_AddressChange_Extract")
    
    # Apply Spark configurations
    for key, value in config.get_spark_config().items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def main():
    """Main pipeline execution."""
    print("=" * 80)
    print("E2E Policy Services - DP Address Change YUYU Extract")
    print("=" * 80)
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Initialize components
        loader = DataLoader(spark)
        transformer = YUYUTransformations()
        writer = DataWriter()
        
        # Step 1: Load source data
        print("\n[1/6] Loading staging data...")
        staging_df = loader.load_staging_data()
        print(f"      Loaded {staging_df.count()} records from staging table")
        
        # Step 2: Load lookup tables
        print("\n[2/6] Loading lookup tables...")
        lookup_clnt_df = loader.load_lookup_yuyu_clnt()
        lookup_cln_df = loader.load_lookup_yuyuk_cln()
        print(f"      Loaded {lookup_clnt_df.count()} records from T_YUYU_CLNT")
        print(f"      Loaded {lookup_cln_df.count()} records from T_YUYUK_CLN")
        
        # Step 3: Enrich with lookups
        print("\n[3/6] Enriching data with lookups...")
        enriched_df = loader.enrich_with_lookups(
            staging_df, 
            lookup_clnt_df, 
            lookup_cln_df
        )
        
        # Step 4: Apply transformations
        print("\n[4/6] Applying business rule transformations...")
        transformed_df = transformer.apply_all_transformations(enriched_df)
        
        # Step 5: Write flat file output
        print("\n[5/6] Writing flat file output...")
        flat_file_df = transformer.select_flat_file_columns(transformed_df)
        writer.write_flat_file_output(flat_file_df)
        print(f"      Output records: {flat_file_df.count()}")
        
        # Step 6: Update staging table
        print("\n[6/6] Updating staging table...")
        staging_update_df = transformer.select_staging_update_columns(transformed_df)
        writer.update_staging_table(staging_update_df)
        
        print("\n" + "=" * 80