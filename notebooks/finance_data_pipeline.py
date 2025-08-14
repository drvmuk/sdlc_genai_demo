# Databricks notebook source
# MAGIC %md
# MAGIC # Finance Data Pipeline
# MAGIC 
# MAGIC **Technical Requirement ID:** TR-FIN-001  
# MAGIC **Related Functional Requirement(s):** FR-FIN-001
# MAGIC 
# MAGIC This notebook implements a data pipeline to populate the Finance table with transformed data from FAGLFLEXA and BSEG source tables in the Everest ECC schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Imports

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from datetime import datetime
from delta.tables import DeltaTable

# Configure logging
log_path = "/dbfs/logs/finance_pipeline/"
log_file = f"finance_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=f"{log_path}{log_file}",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration

# COMMAND ----------

# Configuration parameters
config = {
    "source": {
        "ecc_schema": "everest_ecc",
        "faglflexa_table": "FAGLFLEXA",
        "bseg_table": "BSEG",
        "golden_schema": "golden_views",
        "entity_view": "entity",
        "gl_view": "gl_account",
        "trading_partner_view": "trading_partner"
    },
    "target": {
        "schema": "finance",
        "table": "finance_data",
        "mode": "overwrite",  # Options: append, overwrite, merge
        "format": "parquet"
    },
    "processing": {
        "partition_column": "FiscalYear",
        "checkpoint_location": "/dbfs/checkpoints/finance_pipeline/"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def log_step(step_name, status="Started"):
    """Log the start/end of a processing step"""
    message = f"Step: {step_name} - {status}"
    print(message)
    logging.info(message)

def validate_dataframe(df, df_name):
    """Validate dataframe for basic quality checks"""
    try:
        count = df.count()
        log_step(f"Validation - {df_name}", f"Record count: {count}")
        
        # Check for null values in key columns
        if df_name == "final_df":
            key_columns = ["FiscalYear", "PostingPeriod", "DocumentNumber", "CompCode"]
            for col in key_columns:
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    logging.warning(f"Found {null_count} null values in {col} column")
        
        return True
    except Exception as e:
        logging.error(f"Validation failed for {df_name}: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading Functions

# COMMAND ----------

def load_source_tables():
    """Load source tables from Everest ECC schema"""
    try:
        log_step("Loading source tables")
        
        # Load FAGLFLEXA table
        faglflexa_df = spark.table(f"{config['source']['ecc_schema']}.{config['source']['faglflexa_table']}")
        
        # Load BSEG table
        bseg_df = spark.table(f"{config['source']['ecc_schema']}.{config['source']['bseg_table']}")
        
        # Load Golden Views
        entity_df = spark.table(f"{config['source']['golden_schema']}.{config['source']['entity_view']}")
        gl_df = spark.table(f"{config['source']['golden_schema']}.{config['source']['gl_view']}")
        trading_partner_df = spark.table(f"{config['source']['golden_schema']}.{config['source']['trading_partner_view']}")
        
        log_step("Loading source tables", "Completed")
        
        return {
            "faglflexa": faglflexa_df,
            "bseg": bseg_df,
            "entity": entity_df,
            "gl": gl_df,
            "trading_partner": trading_partner_df
        }
    except Exception as e:
        logging.error(f"Error loading source tables: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Functions

# COMMAND ----------

def transform_financial_data(source_tables):
    """Apply transformation logic to derive required fields"""
    try:
        log_step("Transforming financial data")
        
        faglflexa_df = source_tables["faglflexa"]
        bseg_df = source_tables["bseg"]
        entity_df = source_tables["entity"]
        gl_df = source_tables["gl"]
        trading_partner_df = source_tables["trading_partner"]
        
        # Register temporary views for SQL operations
        faglflexa_df.createOrReplaceTempView("faglflexa")
        bseg_df.createOrReplaceTempView("bseg")
        entity_df.createOrReplaceTempView("entity")
        gl_df.createOrReplaceTempView("gl_account")
        trading_partner_df.createOrReplaceTempView("trading_partner")
        
        # Execute transformation using Spark SQL
        transformed_df = spark.sql("""
            WITH financial_base AS (
                SELECT
                    -- Basic document information
                    CAST(f.RYEAR AS INT) AS FiscalYear,
                    CAST(f.POPER AS INT) AS PostingPeriod,
                    f.BELNR AS DocumentNumber,
                    f.BUKRS AS CompCode,
                    
                    -- Entity information
                    f.BUKRS AS LegalEntityCode,
                    e.golden_entity_id AS LegalEntity,
                    
                    -- GL Account information
                    f.RACCT AS GLAccount,
                    gl.golden_gl_account_id AS GoldenGLAcct,
                    
                    -- Trading Partner information
                    f.KUNNR AS TradingPartner,
                    tp.golden_trading_partner_id AS GoldenTradingPartner,
                    
                    -- Currency and amount fields
                    CAST(f.HSL AS DECIMAL(17,2)) AS GainLossLC,
                    f.RHCUR AS LocalCurrency,
                    CAST(f.KSL AS DECIMAL(17,2)) AS GainLossTC,
                    f.RKCUR AS TransactionCurrency,
                    
                    -- Additional fields
                    CAST(f.DRCRK AS STRING) AS DebitCreditIndicator,
                    CAST(f.PRCTR AS STRING) AS ProfitCenter,
                    CAST(f.KOKRS AS STRING) AS ControllingArea
                FROM 
                    faglflexa f
                LEFT JOIN 
                    entity e ON f.BUKRS = e.source_entity_code
                LEFT JOIN 
                    gl_account gl ON f.RACCT = gl.source_gl_account_code
                LEFT JOIN 
                    trading_partner tp ON f.KUNNR = tp.source_trading_partner_code
                WHERE 
                    f.RYEAR >= 2020  -- Filter for recent years only
            ),
            offset_accounts AS (
                SELECT
                    b.GJAHR AS FiscalYear,
                    b.MONAT AS PostingPeriod,
                    b.BELNR AS DocumentNumber,
                    b.BUKRS AS CompCode,
                    b.HKONT AS OffsetAccount,
                    gl.golden_gl_account_id AS GoldenOffsetAccount,
                    CAST(b.DMBTR AS DECIMAL(17,2)) AS OffsetAccountLCAmount,
                    CAST(b.WRBTR AS DECIMAL(17,2)) AS OffsetAccountTCAmount,
                    b.AUGBL AS OffsetClearingDocumentNumber
                FROM 
                    bseg b
                LEFT JOIN 
                    gl_account gl ON b.HKONT = gl.source_gl_account_code
                WHERE 
                    b.GJAHR >= 2020  -- Filter for recent years only
            )
            
            SELECT
                fb.FiscalYear,
                fb.PostingPeriod,
                fb.DocumentNumber,
                fb.CompCode,
                fb.LegalEntityCode,
                fb.LegalEntity,
                fb.GLAccount,
                fb.GoldenGLAcct,
                fb.TradingPartner,
                fb.GoldenTradingPartner,
                -- Calculate GainLossGC based on LC and TC amounts
                CASE 
                    WHEN fb.GainLossLC IS NOT NULL AND fb.GainLossTC IS NOT NULL 
                    THEN CAST(fb.GainLossLC - fb.GainLossTC AS DECIMAL(17,2))
                    ELSE CAST(NULL AS DECIMAL(17,2))
                END AS GainLossGC,
                fb.GainLossLC,
                fb.LocalCurrency,
                fb.GainLossTC,
                fb.TransactionCurrency,
                oa.OffsetAccount,
                oa.GoldenOffsetAccount,
                oa.OffsetAccountLCAmount,
                oa.OffsetAccountTCAmount,
                oa.OffsetClearingDocumentNumber,
                -- Additional metadata
                fb.DebitCreditIndicator,
                fb.ProfitCenter,
                fb.ControllingArea,
                current_timestamp() AS ProcessedTimestamp
            FROM 
                financial_base fb
            LEFT JOIN 
                offset_accounts oa 
                ON fb.FiscalYear = oa.FiscalYear
                AND fb.PostingPeriod = oa.PostingPeriod
                AND fb.DocumentNumber = oa.DocumentNumber
                AND fb.CompCode = oa.CompCode
        """)
        
        # Validate the transformed dataframe
        if not validate_dataframe(transformed_df, "transformed_df"):
            raise Exception("Data validation failed for transformed dataframe")
        
        log_step("Transforming financial data", "Completed")
        
        return transformed_df
    except Exception as e:
        logging.error(f"Error transforming financial data: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing Functions

# COMMAND ----------

def write_to_target(transformed_df):
    """Write transformed data to target table"""
    try:
        log_step("Writing data to target")
        
        target_table = f"{config['target']['schema']}.{config['target']['table']}"
        write_mode = config['target']['mode']
        
        # Optimize by repartitioning on the partition column
        partitioned_df = transformed_df.repartition(
            F.col(config['processing']['partition_column'])
        )
        
        # Write data to target
        if write_mode == "merge" and spark._jsparkSession.catalog().tableExists(target_table):
            # For merge mode, use Delta Lake merge capabilities
            delta_table = DeltaTable.forName(spark, target_table)
            
            # Define merge condition
            merge_condition = """
                target.FiscalYear = source.FiscalYear AND
                target.PostingPeriod = source.PostingPeriod AND
                target.DocumentNumber = source.DocumentNumber AND
                target.CompCode = source.CompCode AND
                target.GLAccount = source.GLAccount
            """
            
            # Perform merge operation
            delta_table.alias("target").merge(
                partitioned_df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            
            logging.info(f"Merged data into target table: {target_table}")
        else:
            # For append or overwrite modes, use standard write
            partitioned_df.write \
                .format(config['target']['format']) \
                .mode(write_mode) \
                .partitionBy(config['processing']['partition_column']) \
                .option("path", f"/mnt/data/{config['target']['schema']}/{config['target']['table']}") \
                .saveAsTable(target_table)
            
            logging.info(f"Wrote data to target table: {target_table} using {write_mode} mode")
        
        # Get count of records written
        count = spark.table(target_table).count()
        logging.info(f"Total records in target table after write: {count}")
        
        log_step("Writing data to target", "Completed")
        
        return True
    except Exception as e:
        logging.error(f"Error writing data to target: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Pipeline Execution

# COMMAND ----------

def run_finance_pipeline():
    """Main function to orchestrate the finance data pipeline"""
    start_time = datetime.now()
    log_step("Finance Data Pipeline", "Started")
    
    try:
        # Step 1: Load source tables
        source_tables = load_source_tables()
        
        # Step 2: Transform financial data
        transformed_df = transform_financial_data(source_tables)
        
        # Step 3: Write transformed data to target
        write_to_target(transformed_df)
        
        # Log successful completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        log_step("Finance Data Pipeline", f"Completed successfully in {duration:.2f} seconds")
        
        return {
            "status": "success",
            "duration_seconds": duration,
            "records_processed": transformed_df.count(),
            "timestamp": end_time.strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        # Log failure
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        error_msg = f"Failed after {duration:.2f} seconds: {str(e)}"
        logging.error(error_msg)
        log_step("Finance Data Pipeline", "Failed")
        
        return {
            "status": "failed",
            "error": str(e),
            "duration_seconds": duration,
            "timestamp": end_time.strftime('%Y-%m-%d %H:%M:%S')
        }

# COMMAND ----------

# Execute the pipeline
result = run_finance_pipeline()
displayHTML(f"""
<h3>Pipeline Execution Result</h3>
<table>
  <tr><td><b>Status:</b></td><td>{result['status']}</td></tr>
  <tr><td><b>Duration:</b></td><td>{result['duration_seconds']:.2f} seconds</td></tr>
  <tr><td><b>Timestamp:</b></td><td>{result['timestamp']}</td></tr>
  {'<tr><td><b>Records:</b></td><td>' + str(result['records_processed']) + '</td></tr>' if 'records_processed' in result else ''}
  {'<tr><td><b>Error:</b></td><td>' + result['error'] + '</td></tr>' if 'error' in result else ''}
</table>
""")