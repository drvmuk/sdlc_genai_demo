from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, DecimalType
import datetime
import logging

def get_spark_session():
    """
    Create and return a SparkSession
    """
    return SparkSession.builder \
        .appName("R4B Acquisition Contract Job") \
        .getOrCreate()

def create_schema_and_table(spark):
    """
    Create schema and table if they don't exist
    """
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS drvd__app_r4b")
        
        # Define the schema for r4b_acq_contract table
        table_schema = """
        CREATE TABLE IF NOT EXISTS drvd__app_r4b.r4b_acq_contract (
            LINE_KEY STRING,
            BAN STRING,
            SUBSCRIBER_NO STRING,
            INIT_ACTIVATION_DATE DATE,
            PLAN_NAME STRING,
            PLAN_CODE STRING,
            PLAN_PRICE DECIMAL(18,2),
            PLAN_CATEGORY STRING,
            PLAN_TYPE STRING,
            PLAN_SUBTYPE STRING,
            CONTRACT_TYPE STRING,
            CONTRACT_LENGTH INT,
            CONTRACT_START_DATE DATE,
            CONTRACT_END_DATE DATE,
            INIT_DATE DATE,
            END_DATE DATE,
            CREATED_TS TIMESTAMP,
            CREATED_BY STRING,
            UPDATED_TS TIMESTAMP,
            UPDATED_BY STRING
        ) USING PARQUET
        """
        
        spark.sql(table_schema)
        logging.info("Schema and table creation successful")
        return True
    except Exception as e:
        logging.error(f"Error creating schema and table: {str(e)}")
        raise

def process_flow_1(spark):
    """
    Process Flow 1: exp_FIRST_VALUE_PASS_THROUGH
    """
    try:
        flow_1_df = spark.table("R4B_SUB_ACQUISITION_FACT_STG") \
            .join(
                spark.table("CONTRACT"),
                (F.col("R4B_SUB_ACQUISITION_FACT_STG.BAN") == F.col("CONTRACT.BAN")) &
                (F.col("R4B_SUB_ACQUISITION_FACT_STG.SUBSCRIBER_NO") == F.col("CONTRACT.SUBSCRIBER_NO")),
                "inner"
            ) \
            .filter(F.col("CONTRACT.CONTRACT_TYPE").isin("NEW", "UPGRADE", "MNP")) \
            .select(
                F.col("R4B_SUB_ACQUISITION_FACT_STG.LINE_KEY"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.BAN"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.SUBSCRIBER_NO"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.INIT_ACTIVATION_DATE"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_NAME"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_CODE"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_PRICE"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_CATEGORY"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_TYPE"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_SUBTYPE"),
                F.col("CONTRACT.CONTRACT_TYPE"),
                F.col("CONTRACT.CONTRACT_LENGTH"),
                F.col("CONTRACT.CONTRACT_START_DATE"),
                F.col("CONTRACT.CONTRACT_END_DATE")
            )
        
        logging.info(f"Flow 1 processed with {flow_1_df.count()} records")
        return flow_1_df
    except Exception as e:
        logging.error(f"Error processing Flow 1: {str(e)}")
        raise

def process_flow_2(spark):
    """
    Process Flow 2: exp_PROCESSING_DAY_PASS_THROUGH
    """
    try:
        flow_2_df = spark.table("R4B_SUB_ACQUISITION_FACT_STG") \
            .join(
                spark.table("CONTRACT"),
                (F.col("R4B_SUB_ACQUISITION_FACT_STG.BAN") == F.col("CONTRACT.BAN")) &
                (F.col("R4B_SUB_ACQUISITION_FACT_STG.SUBSCRIBER_NO") == F.col("CONTRACT.SUBSCRIBER_NO")),
                "inner"
            ) \
            .filter(F.col("CONTRACT.CONTRACT_TYPE").isin("RETENTION")) \
            .select(
                F.col("R4B_SUB_ACQUISITION_FACT_STG.LINE_KEY"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.BAN"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.SUBSCRIBER_NO"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.INIT_ACTIVATION_DATE"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_NAME"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_CODE"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_PRICE"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_CATEGORY"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_TYPE"),
                F.col("R4B_SUB_ACQUISITION_FACT_STG.PLAN_SUBTYPE"),
                F.col("CONTRACT.CONTRACT_TYPE"),
                F.col("CONTRACT.CONTRACT_LENGTH"),
                F.col("CONTRACT.CONTRACT_START_DATE"),
                F.col("CONTRACT.CONTRACT_END_DATE")
            )
        
        logging.info(f"Flow 2 processed with {flow_2_df.count()} records")
        return flow_2_df
    except Exception as e:
        logging.error(f"Error processing Flow 2: {str(e)}")
        raise

def join_and_transform(flow_1_df, flow_2_df):
    """
    Perform full outer join between Flow 1 and Flow 2
    """
    try:
        joined_df = flow_1_df.join(
            flow_2_df,
            (flow_1_df["LINE_KEY"] == flow_2_df["LINE_KEY"]) &
            (flow_1_df["INIT_ACTIVATION_DATE"] == flow_2_df["INIT_ACTIVATION_DATE"]),
            "full"
        )
        
        # Select and transform columns
        transformed_df = joined_df.select(
            F.coalesce(flow_1_df["LINE_KEY"], flow_2_df["LINE_KEY"]).alias("LINE_KEY"),
            F.coalesce(flow_1_df["BAN"], flow_2_df["BAN"]).alias("BAN"),
            F.coalesce(flow_1_df["SUBSCRIBER_NO"], flow_2_df["SUBSCRIBER_NO"]).alias("SUBSCRIBER_NO"),
            F.coalesce(flow_1_df["INIT_ACTIVATION_DATE"], flow_2_df["INIT_ACTIVATION_DATE"]).alias("INIT_ACTIVATION_DATE"),
            F.coalesce(flow_1_df["PLAN_NAME"], flow_2_df["PLAN_NAME"]).alias("PLAN_NAME"),
            F.coalesce(flow_1_df["PLAN_CODE"], flow_2_df["PLAN_CODE"]).alias("PLAN_CODE"),
            F.coalesce(flow_1_df["PLAN_PRICE"], flow_2_df["PLAN_PRICE"]).alias("PLAN_PRICE"),
            F.coalesce(flow_1_df["PLAN_CATEGORY"], flow_2_df["PLAN_CATEGORY"]).alias("PLAN_CATEGORY"),
            F.coalesce(flow_1_df["PLAN_TYPE"], flow_2_df["PLAN_TYPE"]).alias("PLAN_TYPE"),
            F.coalesce(flow_1_df["PLAN_SUBTYPE"], flow_2_df["PLAN_SUBTYPE"]).alias("PLAN_SUBTYPE"),
            F.coalesce(flow_1_df["CONTRACT_TYPE"], flow_2_df["CONTRACT_TYPE"]).alias("CONTRACT_TYPE"),
            F.coalesce(flow_1_df["CONTRACT_LENGTH"], flow_2_df["CONTRACT_LENGTH"]).alias("CONTRACT_LENGTH"),
            F.coalesce(flow_1_df["CONTRACT_START_DATE"], flow_2_df["CONTRACT_START_DATE"]).alias("CONTRACT_START_DATE"),
            F.coalesce(flow_1_df["CONTRACT_END_DATE"], flow_2_df["CONTRACT_END_DATE"]).alias("CONTRACT_END_DATE")
        )
        
        logging.info(f"Join and transform completed with {transformed_df.count()} records")
        return transformed_df
    except Exception as e:
        logging.error(f"Error in join and transform: {str(e)}")
        raise

def apply_watermarking_and_deletion(spark, transformed_df):
    """
    Apply watermarking and deletion logic using margin_control table
    """
    try:
        # Get margin control values
        margin_df = spark.table("margin_control")
        margin_row = margin_df.collect()[0]
        processing_date = margin_row["PROCESSING_DATE"]
        margin_days = margin_row["MARGIN_DAYS"]
        
        # Calculate INIT_DATE and END_DATE
        final_df = transformed_df.withColumn(
            "INIT_DATE", 
            F.date_sub(F.lit(processing_date), margin_days)
        ).withColumn(
            "END_DATE", 
            F.date_add(F.lit(processing_date), margin_days)
        )
        
        # Identify records to delete
        delete_condition = f"""
        DELETE FROM drvd__app_r4b.r4b_acq_contract
        WHERE LINE_KEY IN (
            SELECT LINE_KEY FROM drvd__app_r4b.r4b_acq_contract
            WHERE INIT_DATE <= '{processing_date}' AND END_DATE >= '{processing_date}'
        )
        """
        
        # Execute deletion
        spark.sql(delete_condition)
        
        logging.info("Watermarking and deletion logic applied successfully")
        return final_df
    except Exception as e:
        logging.error(f"Error applying watermarking and deletion: {str(e)}")
        raise

def write_final_data(spark, final_df):
    """
    Add metadata columns and write final data to target table
    """
    try:
        current_timestamp = F.current_timestamp()
        current_user = F.lit(spark.sparkContext.sparkUser())
        
        # Add metadata columns
        final_df_with_metadata = final_df.withColumn(
            "CREATED_TS", current_timestamp
        ).withColumn(
            "CREATED_BY", current_user
        ).withColumn(
            "UPDATED_TS", current_timestamp
        ).withColumn(
            "UPDATED_BY", current_user
        )
        
        # Write to target table
        final_df_with_metadata.write.mode("append").format("parquet").saveAsTable("drvd__app_r4b.r4b_acq_contract")
        
        row_count = final_df_with_metadata.count()
        logging.info(f"Successfully wrote {row_count} records to drvd__app_r4b.r4b_acq_contract")
        return row_count
    except Exception as e:
        logging.error(f"Error writing final data: {str(e)}")
        raise

def run_r4b_acq_contract_job():
    """
    Main function to run the R4B Acquisition Contract job
    """
    start_time = datetime.datetime.now()
    logging.info(f"Starting R4B Acquisition Contract job at {start_time}")
    
    spark = get_spark_session()
    
    try:
        # Step 1: Create schema and table if they don't exist
        create_schema_and_table(spark)
        
        # Step 2: Process Flow 1
        flow_1_df = process_flow_1(spark)
        
        # Step 3: Process Flow 2
        flow_2_df = process_flow_2(spark)
        
        # Step 4: Join and transform data
        transformed_df = join_and_transform(flow_1_df, flow_2_df)
        
        # Step 5: Apply watermarking and deletion logic
        final_df = apply_watermarking_and_deletion(spark, transformed_df)
        
        # Step 6: Write final data
        row_count = write_final_data(spark, final_df)
        
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logging.info(f"R4B Acquisition Contract job completed successfully at {end_time}")
        logging.info(f"Total duration: {duration} seconds")
        logging.info(f"Total records processed: {row_count}")
        
        return {
            "status": "success",
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration,
            "records_processed": row_count
        }
    
    except Exception as e:
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logging.error(f"R4B Acquisition Contract job failed at {end_time}")
        logging.error(f"Error: {str(e)}")
        
        return {
            "status": "failed",
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration,
            "error": str(e)
        }

if __name__ == "__main__":
    run_r4b_acq_contract_job()