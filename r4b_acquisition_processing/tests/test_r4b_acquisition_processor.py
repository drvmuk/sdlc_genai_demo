"""
Unit tests for R4B Acquisition Contract Data Processing
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from datetime import datetime, timedelta
import os
import tempfile
import shutil
from src.r4b_acquisition_processor import R4BAcquisitionProcessor

class TestR4BAcquisitionProcessor(unittest.TestCase):
    """Test cases for R4BAcquisitionProcessor"""

    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("R4B Acquisition Contract Data Processing Test") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
            
        # Create test data directory
        cls.test_data_dir = tempfile.mkdtemp()
        
        # Create test data
        cls._create_test_data()
        
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        # Stop Spark session
        cls.spark.stop()
        
        # Remove test data directory
        shutil.rmtree(cls.test_data_dir)
    
    @classmethod
    def _create_test_data(cls):
        """Create test data for the tests"""
        # Create acquisition data
        acquisition_schema = StructType([
            StructField("LINE_KEY", StringType(), False),
            StructField("INIT_ACTIVATION_DATE", TimestampType(), True),
            StructField("END_DATE", TimestampType(), True),
            StructField("CONTRACT_ID", StringType(), True)
        ])
        
        now = datetime.now()
        acquisition_data = [
            ("LINE001", now, now + timedelta(days=365), "CONTRACT001"),
            ("LINE002", now, now + timedelta(days=180), "CONTRACT002"),
            ("LINE003", now - timedelta(days=30), now + timedelta(days=335), "CONTRACT003"),
            ("LINE004", now - timedelta(days=60), now + timedelta(days=305), "CONTRACT004")
        ]
        
        acquisition_df = cls.spark.createDataFrame(acquisition_data, acquisition_schema)
        acquisition_path = os.path.join(cls.test_data_dir, "r4b/staging")
        acquisition_df.write.format("delta").save(acquisition_path)
        
        # Create contract data
        contract_schema = StructType([
            StructField("CONTRACT_ID", StringType(), False),
            StructField("PRODUCT_TYPE", StringType(), True),
            StructField("EFFECTIVE_DATE", TimestampType(), True),
            StructField("COMMIT_END_DATE", TimestampType(), True),
            StructField("COMMITMENT_TERM_MONTHS", IntegerType(), True),
            StructField("COMMITMENT_AMOUNT", DoubleType(), True),
            StructField("COMMITMENT_TYPE", StringType(), True),
            StructField("SOURCE_SYSTEM", StringType(), True),
            StructField("GG_OP_TYPE", StringType(), True)
        ])
        
        contract_data = [
            ("CONTRACT001", "C", now - timedelta(days=10), now + timedelta(days=720), 24, 1000.0, "STANDARD", "CRM", "I"),
            ("CONTRACT002", "A", now - timedelta(days=5), now + timedelta(days=360), 12, 500.0, "PROMO", "CRM", "I"),
            ("CONTRACT003", "C", now - timedelta(days=40), now + timedelta(days=680), 24, 1200.0, "STANDARD", "CRM", "I"),
            ("CONTRACT004", "A", now - timedelta(days=70), now + timedelta(days=650), 24, 800.0, "PROMO", "CRM", "I"),
            ("CONTRACT005", "B", now - timedelta(days=15), now + timedelta(days=350), 12, 300.0, "STANDARD", "CRM", "I"),
            ("CONTRACT006", "C", now - timedelta(days=20), now + timedelta(days=710), 24, 1100.0, "STANDARD", "CRM", "D")
        ]
        
        contract_df = cls.spark.createDataFrame(contract_data, contract_schema)
        contract_path = os.path.join(cls.test_data_dir, "contracts")
        contract_df.write.format("delta").save(contract_path)
        
        # Create control data
        control_schema = StructType([
            StructField("control_name", StringType(), False),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("parameter_value", StringType(), True)
        ])
        
        control_data = [
            ("r4b_acquisition_watermark", now - timedelta(days=90), now, "ACTIVE")
        ]
        
        control_df = cls.spark.createDataFrame(control_data, control_schema)
        control_path = os.path.join(cls.test_data_dir, "control")
        control_df.write.format("delta").save(control_path)
    
    def setUp(self):
        """Set up test case"""
        # Create processor instance with test paths
        self.processor = R4BAcquisitionProcessor(self.spark)
        self.processor.source_paths = {
            "acquisition": os.path.join(self.test_data_dir, "r4b/staging"),
            "contract": os.path.join(self.test_data_dir, "contracts"),
            "control": os.path.join(self.test_data_dir, "control")
        }
        
        # Create test schema
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.processor.target_schema}")
    
    def test_extract_data(self):
        """Test data extraction"""
        acquisition_df, contract_df, control_df = self.processor.extract_data()
        
        # Verify acquisition data
        self.assertEqual(acquisition_df.count(), 4)
        self.assertTrue("LINE_KEY" in acquisition_df.columns)
        self.assertTrue("CONTRACT_ID" in acquisition_df.columns)
        
        # Verify contract data
        self.assertEqual(contract_df.count(), 6)
        self.assertTrue("CONTRACT_ID" in contract_df.columns)
        self.assertTrue("PRODUCT_TYPE" in contract_df.columns)
        
        # Verify control data
        self.assertEqual(control_df.count(), 1)
        self.assertTrue("control_name" in control_df.columns)
    
    def test_process_flow_1(self):
        """Test Flow 1 processing"""
        acquisition_df, contract_df, _ = self.processor.extract_data()
        flow1_df = self.processor.process_flow_1(acquisition_df, contract_df)
        
        # Verify Flow 1 results
        self.assertTrue(flow1_df.count() > 0)
        self.assertTrue("PROCESS_TYPE" in flow1_df.columns)
        
        # Verify all records have FLOW_1 process type
        process_types = flow1_df.select("PROCESS_TYPE").distinct().collect()
        self.assertEqual(len(process_types), 1)
        self.assertEqual(process_types[0]["PROCESS_TYPE"], "FLOW_1")
        
        # Verify only valid product types are included
        product_types = flow1_df.select("PRODUCT_TYPE").distinct().collect()
        for row in product_types:
            self.assertIn(row["PRODUCT_TYPE"], ["C", "A"])
    
    def test_process_flow_2(self):
        """Test Flow 2 processing"""
        acquisition_df, contract_df, _ = self.processor.extract_data()
        flow2_df = self.processor.process_flow_2(acquisition_df, contract_df)
        
        # Verify Flow 2 results
        self.assertTrue(flow2_df.count() > 0)
        self.assertTrue("PROCESS_TYPE" in flow2_df.columns)
        
        # Verify all records have FLOW_2 process type
        process_types = flow2_df.select("PROCESS_TYPE").distinct().collect()
        self.assertEqual(len(process_types), 1)
        self.assertEqual(process_types[0]["PROCESS_TYPE"], "FLOW_2")
        
        # Verify only valid product types are included
        product_types = flow2_df.select("PRODUCT_TYPE").distinct().collect()
        for row in product_types:
            self.assertIn(row["PRODUCT_TYPE"], ["C", "A"])
    
    def test_join_and_transform(self):
        """Test join and transform logic"""
        acquisition_df, contract_df, _ = self.processor.extract_data()
        flow1_df = self.processor.process_flow_1(acquisition_df, contract_df)
        flow2_df = self.processor.process_flow_2(acquisition_df, contract_df)
        combined_df = self.processor.join_and_transform(flow1_df, flow2_df)
        
        # Verify combined results
        self.assertTrue(combined_df.count() > 0)
        
        # Verify required columns exist
        required_columns = ["LINE_KEY", "INIT_ACTIVATION_DATE", "CONTRACT_ID", "PROCESS_TYPE"]
        for col in required_columns:
            self.assertTrue(col in combined_df.columns)
    
    def test_apply_watermarking(self):
        """Test watermarking logic"""
        acquisition_df, contract_df, control_df = self.processor.extract_data()
        flow1_df = self.processor.process_flow_1(acquisition_df, contract_df)
        flow2_df = self.processor.process_flow_2(acquisition_df, contract_df)
        combined_df = self.processor.join_and_transform(flow1_df, flow2_df)
        final_df, start_date, end_date = self.processor.apply_watermarking(combined_df, control_df)
        
        # Verify watermark results
        self.assertIsNotNone(start_date)
        self.assertIsNotNone(end_date)
        self.assertTrue(isinstance(start_date, datetime))
        self.assertTrue(isinstance(end_date, datetime))
        
        # Verify all records are within watermark range
        if final_df.count() > 0:
            min_date = final_df.agg(F.min("INIT_ACTIVATION_DATE")).collect()[0][0]
            max_date = final_df.agg(F.max("INIT_ACTIVATION_DATE")).collect()[0][0]
            self.assertTrue(min_date >= start_date)
            self.assertTrue(max_date <= end_date)
    
    def test_create_schema_and_table(self):
        """Test schema and table creation"""
        self.processor.create_schema_and_table()
        
        # Verify schema exists
        schemas = self.spark.sql("SHOW SCHEMAS").collect()
        schema_exists = any(row.databaseName == self.processor.target_schema for row in schemas)
        self.assertTrue(schema_exists)
        
        # Verify table exists
        tables = self.spark.sql(f"SHOW TABLES IN {self.processor.target_schema}").collect()
        table_exists = any(row.tableName == self.processor.target_table for row in tables)
        self.assertTrue(table_exists)

if __name__ == '__main__':
    unittest.main()