import unittest
from pyspark.sql import SparkSession
import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType

from src.r4b_acq_contract_job import (
    create_schema_and_table,
    process_flow_1,
    process_flow_2,
    join_and_transform,
    apply_watermarking_and_deletion,
    write_final_data
)

class TestR4BAcqContractJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("R4B Acquisition Contract Job Test") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create test data
        cls._create_test_data()
    
    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()
    
    @classmethod
    def _create_test_data(cls):
        # Create R4B_SUB_ACQUISITION_FACT_STG test data
        r4b_schema = StructType([
            StructField("LINE_KEY", StringType(), True),
            StructField("BAN", StringType(), True),
            StructField("SUBSCRIBER_NO", StringType(), True),
            StructField("INIT_ACTIVATION_DATE", DateType(), True),
            StructField("PLAN_NAME", StringType(), True),
            StructField("PLAN_CODE", StringType(), True),
            StructField("PLAN_PRICE", DecimalType(18, 2), True),
            StructField("PLAN_CATEGORY", StringType(), True),
            StructField("PLAN_TYPE", StringType(), True),
            StructField("PLAN_SUBTYPE", StringType(), True)
        ])
        
        r4b_data = [
            ("L001", "B001", "S001", datetime.date(2023, 1, 1), "Plan A", "PA001", 
             decimal.Decimal("49.99"), "Consumer", "Postpaid", "Voice"),
            ("L002", "B002", "S002", datetime.date(2023, 1, 2), "Plan B", "PB001", 
             decimal.Decimal("59.99"), "Business", "Postpaid", "Data"),
            ("L003", "B003", "S003", datetime.date(2023, 1, 3), "Plan C", "PC001", 
             decimal.Decimal("39.99"), "Consumer", "Prepaid", "Voice")
        ]
        
        cls.r4b_df = cls.spark.createDataFrame(r4b_data, r4b_schema)
        cls.r4b_df.createOrReplaceTempView("R4B_SUB_ACQUISITION_FACT_STG")
        
        # Create CONTRACT test data
        contract_schema = StructType([
            StructField("BAN", StringType(), True),
            StructField("SUBSCRIBER_NO", StringType(), True),
            StructField("CONTRACT_TYPE", StringType(), True),
            StructField("CONTRACT_LENGTH", IntegerType(), True),
            StructField("CONTRACT_START_DATE", DateType(), True),
            StructField("CONTRACT_END_DATE", DateType(), True)
        ])
        
        contract_data = [
            ("B001", "S001", "NEW", 24, datetime.date(2023, 1, 1), datetime.date(2025, 1, 1)),
            ("B002", "S002", "UPGRADE", 12, datetime.date(2023, 1, 2), datetime.date(2024, 1, 2)),
            ("B003", "S003", "RETENTION", 24, datetime.date(2023, 1, 3), datetime.date(2025, 1, 3))
        ]
        
        cls.contract_df = cls.spark.createDataFrame(contract_data, contract_schema)
        cls.contract_df.createOrReplaceTempView("CONTRACT")
        
        # Create margin_control test data
        margin_schema = StructType([
            StructField("PROCESSING_DATE", DateType(), True),
            StructField("MARGIN_DAYS", IntegerType(), True)
        ])
        
        margin_data = [
            (datetime.date(2023, 2, 1), 30)
        ]
        
        cls.margin_df = cls.spark.createDataFrame(margin_data, margin_schema)
        cls.margin_df.createOrReplaceTempView("margin_control")
        
        # Create target table for testing
        cls.spark.sql("""
        CREATE DATABASE IF NOT EXISTS drvd__app_r4b
        """)
        
        cls.spark.sql("""
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
        )
        """)
    
    def test_create_schema_and_table(self):
        result = create_schema_and_table(self.spark)
        self.assertTrue(result)
        
        # Verify table exists
        tables = self.spark.sql("SHOW TABLES IN drvd__app_r4b").collect()
        table_names = [row.tableName for row in tables]
        self.assertIn("r4b_acq_contract", table_names)
    
    def test_process_flow_1(self):
        flow_1_df = process_flow_1(self.spark)
        
        # Verify the output
        self.assertEqual(flow_1_df.count(), 2)  # Only NEW and UPGRADE records
        
        # Check if the columns are correct
        expected_columns = ["LINE_KEY", "BAN", "SUBSCRIBER_NO", "INIT_ACTIVATION_DATE", 
                           "PLAN_NAME", "PLAN_CODE", "PLAN_PRICE", "PLAN_CATEGORY", 
                           "PLAN_TYPE", "PLAN_SUBTYPE", "CONTRACT_TYPE", "CONTRACT_LENGTH",
                           "CONTRACT_START_DATE", "CONTRACT_END_DATE"]
        self.assertEqual(flow_1_df.columns, expected_columns)
        
        # Check if filtering worked correctly
        contract_types = [row.CONTRACT_TYPE for row in flow_1_df.select("CONTRACT_TYPE").distinct().collect()]
        self.assertIn("NEW", contract_types)
        self.assertIn("UPGRADE", contract_types)
        self.assertNotIn("RETENTION", contract_types)
    
    def test_process_flow_2(self):
        flow_2_df = process_flow_2(self.spark)
        
        # Verify the output
        self.assertEqual(flow_2_df.count(), 1)  # Only RETENTION records
        
        # Check if the columns are correct
        expected_columns = ["LINE_KEY", "BAN", "SUBSCRIBER_NO", "INIT_ACTIVATION_DATE", 
                           "PLAN_NAME", "PLAN_CODE", "PLAN_PRICE", "PLAN_CATEGORY", 
                           "PLAN_TYPE", "PLAN_SUBTYPE", "CONTRACT_TYPE", "CONTRACT_LENGTH",
                           "CONTRACT_START_DATE", "CONTRACT_END_DATE"]
        self.assertEqual(flow_2_df.columns, expected_columns)
        
        # Check if filtering worked correctly
        contract_types = [row.CONTRACT_TYPE for row in flow_2_df.select("CONTRACT_TYPE").distinct().collect()]
        self.assertEqual(contract_types, ["RETENTION"])
    
    def test_join_and_transform(self):
        flow_1_df = process_flow_1(self.spark)
        flow_2_df = process_flow_2(self.spark)
        
        transformed_df = join_and_transform(flow_1_df, flow_2_df)
        
        # Verify the output
        self.assertEqual(transformed_df.count(), 3)  # All records from both flows
        
        # Check if the columns are correct
        expected_columns = ["LINE_KEY", "BAN", "SUBSCRIBER_NO", "INIT_ACTIVATION_DATE", 
                           "PLAN_NAME", "PLAN_CODE", "PLAN_PRICE", "PLAN_CATEGORY", 
                           "PLAN_TYPE", "PLAN_SUBTYPE", "CONTRACT_TYPE", "CONTRACT_LENGTH",
                           "CONTRACT_START_DATE", "CONTRACT_END_DATE"]
        self.assertEqual(transformed_df.columns, expected_columns)
    
    def test_apply_watermarking_and_deletion(self):
        flow_1_df = process_flow_1(self.spark)
        flow_2_df = process_flow_2(self.spark)
        transformed_df = join_and_transform(flow_1_df, flow_2_df)
        
        final_df = apply_watermarking_and_deletion(self.spark, transformed_df)
        
        # Verify the output
        self.assertEqual(final_df.count(), 3)
        
        # Check if INIT_DATE and END_DATE are added
        self.assertIn("INIT_DATE", final_df.columns)
        self.assertIn("END_DATE", final_df.columns)
        
        # Check the values of INIT_DATE and END_DATE
        margin_row = self.spark.table("margin_control").collect()[0]
        processing_date = margin_row["PROCESSING_DATE"]
        margin_days = margin_row["MARGIN_DAYS"]
        
        expected_init_date = processing_date - datetime.timedelta(days=margin_days)
        expected_end_date = processing_date + datetime.timedelta(days=margin_days)
        
        sample_row = final_df.first()
        self.assertEqual(sample_row["INIT_DATE"], expected_init_date)
        self.assertEqual(sample_row["END_DATE"], expected_end_date)
    
    def test_write_final_data(self):
        flow_1_df = process_flow_1(self.spark)
        flow_2_df = process_flow_2(self.spark)
        transformed_df = join_and_transform(flow_1_df, flow_2_df)
        final_df = apply_watermarking_and_deletion(self.spark, transformed_df)
        
        row_count = write_final_data(self.spark, final_df)
        
        # Verify the output
        self.assertEqual(row_count, 3)
        
        # Check if data was written to the table
        table_data = self.spark.table("drvd__app_r4b.r4b_acq_contract")
        self.assertEqual(table_data.count(), 3)
        
        # Check if metadata columns are added
        self.assertIn("CREATED_TS", table_data.columns)
        self.assertIn("CREATED_BY", table_data.columns)
        self.assertIn("UPDATED_TS", table_data.columns)
        self.assertIn("UPDATED_BY", table_data.columns)

if __name__ == "__main__":
    unittest.main()