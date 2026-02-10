"""
Data writer module for outputting results to flat file and updating staging table.
"""

from pyspark.sql import DataFrame
from config import Config


class DataWriter:
    """Handles writing output data to target systems."""
    
    def __init__(self):
        self.config = Config()
    
    def write_flat_file_output(self, df: DataFrame) -> None:
        """
        Write DPAddressChangeYUYU flat file output.
        
        Args:
            df: DataFrame with flat file schema
        """
        output_format = self.config.OUTPUT_FORMAT
        output_path = self.config.FLAT_FILE_OUTPUT_PATH
        
        if output_format == "csv":
            df.write.mode("overwrite").option("header", "true").option(
                "encoding", "UTF-8"
            ).csv(output_path)
        else:  # parquet (default)
            df.write.mode("overwrite").parquet(output_path)
        
        print(f"Flat file output written to: {output_path}")
    
    def update_staging_table(self, df: DataFrame) -> None:
        """
        Update STG_E2E_AC_TXDBH_DATA with process stamps.
        
        Args:
            df: DataFrame with staging update schema
        
        Note:
            In production, this would use JDBC with update mode.
            For PySpark, we simulate by writing to a separate location
            or using a merge/upsert pattern with Delta Lake.
        """
        # Create temporary view for update logic
        df.createOrReplaceTempView("staging_updates")
        
        # In production with Oracle JDBC:
        # df.write.jdbc(
        #     url=self.config.get_oracle_jdbc_url(),
        #     table=self.config.SOURCE_TABLE,
        #     mode="append",  # Would use custom update logic
        #     properties=self.config.get_oracle_properties()
        # )
        
        # For demonstration, write to separate location
        update_path = f"{self.config.OUTPUT_BASE_PATH}/staging_updates"
        df.write.mode("overwrite").parquet(update_path)
        
        print(f"Staging updates written to: {update_path}")
        print(f"Records updated: {df.count()}")