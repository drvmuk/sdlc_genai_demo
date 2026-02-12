"""
Unit tests for E2E BC K2H Data Extraction module.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import datetime
import os
import sys

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.e2e_bc_k2h_extraction import transform_data


class TestE2EBcK2hExtraction(unittest.TestCase):
    """Test cases for E2E BC K2H Data Extraction module."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the Spark session for tests."""
        cls.spark = SparkSession.builder \
            .appName("TestE2EBcK2hExtraction") \
            .master("local[2]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session."""
        cls.spark.stop()
    
    def test_transform_data(self):
        """Test the transform_data function."""
        # Define schema for test data
        schema = StructType([
            StructField("FNL_POLICY_ID", StringType(), True),
            StructField("TR_REQUEST_ID", IntegerType(), True),
            StructField("TR_REQUEST_STATUS", StringType(), True),
            StructField("TR_UPDATE_DATETIME", TimestampType(), True),
            StructField("TTB_REQUEST_ID", IntegerType(), True),
            StructField("TTB_POLICY_ID", StringType(), True),
            StructField("TTB_TRANSACTION_TYPE", StringType(), True),
            StructField("TTB_TRANSACTION_STATUS", StringType(), True),
            StructField("TTB_DISCARD_FLAG", StringType(), True),
            StructField("TTB_TRANSACTION_ID", IntegerType(), True),
            StructField("TRP_TRANSACTION_ID", IntegerType(), True),
            StructField("TRP_POLICY_ID", StringType(), True),
            StructField("TRP_OTHER_POLICY_ID_USE", StringType(), True),
            StructField("TRP_SRC_SYS_MSTR_CD", StringType(), True),
            StructField("TRP_OTHER_POLICY_ID", StringType(), True),
            StructField("TRP_POWN_NAME_NUMBER", StringType(), True),
            StructField("TTR_TRANSACTION_ID", IntegerType(), True),
            StructField("TTR_BENIFICIARY_CHANGE_FLAG", StringType(), True),
            StructField("TTR_OBJECT_SUB_TYPE", StringType(), True),
            StructField("TTR_TRANSACTION_RELATION_ID", IntegerType(), True),
            StructField("TTR_NAME_NUMBER", StringType(), True),
            StructField("TTDBC_REQUEST_ID", IntegerType(), True),
            StructField("TTDBC_TRANSACTION_RELATION_ID", IntegerType(), True),
            StructField("TTDBC_CHANGE_STATE", StringType(), True),
            StructField("TTDBC_LASTNAME_KANJI", StringType(), True),
            StructField("TTDBC_FIRSTNAME_KANJI", StringType(), True),
            StructField("TTDBC_GENDER_CODE", StringType(), True),
            StructField("TTDBC_LASTNAME", StringType(), True),
            StructField("TTDBC_FIRSTNAME", StringType(), True),
            StructField("TTDBC_PERCENTAGE", StringType(), True),
            StructField("TTDBC_EFFECTIVE_DATE", TimestampType(), True),
            StructField("TTDBC_CHANGE_ID", IntegerType(), True),
            StructField("DATA_TYPE", StringType(), True),
            StructField("POLICY_TYPE", StringType(), True),
            StructField("MC_CRNCY", StringType(), True)
        ])
        
        # Create test data
        test_data = [
            (
                "POL123456", 1001, "ACTIVE", datetime.datetime.now(),
                1001, "POL123456", "BENEFICIARY_CHANGE", "COMPLETE", "N", 2001,
                2001, "POL123456", "N", "SRC1", "POL999999", "12345",
                2001, "Y", "BENEFICIARY", 3001, "12345",
                1001, 3001, "NEW", "山田  ", "  太郎", "M",
                "Yamada  ", "  Taro", "100", datetime.datetime.now(), 4001,
                "INDIVIDUAL", "TERM", "JPY"
            ),
            # Test case with null Kanji names
            (
                "POL789012", 1002, "ACTIVE", datetime.datetime.now(),
                1002, "POL789012", "BENEFICIARY_CHANGE", "COMPLETE", "N", 2002,
                2002, "POL789012", "N", "SRC1", "POL888888", "67890",
                2002, "Y", "BENEFICIARY", 3002, "67890",
                1002, 3002, "NEW", None, None, "F",
                "Suzuki", "Hanako", "50", datetime.datetime.now(), 4002,
                "INDIVIDUAL", "TERM", "JPY"
            ),
            # Test case with invalid Kanji characters (will be replaced)
            (
                "POL345678", 1003, "ACTIVE", datetime.datetime.now(),
                1003, "POL345678", "BENEFICIARY_CHANGE", "COMPLETE", "N", 2003,
                2003, "POL345678", "N", "SRC1", "POL777777", "13579",
                2003, "Y", "BENEFICIARY", 3003, "13579",
                1003, 3003, "NEW", "佐藤\r\n", "\u9999\u9999", "M",  # \u9999 is outside valid ranges
                "Sato", "Ichiro", "75", datetime.datetime.now(), 4003,
                "INDIVIDUAL", "TERM", "JPY"
            )
        ]
        
        # Create DataFrame
        source_df = self.spark.createDataFrame(test_data, schema)
        
        # Call the function under test
        result = transform_data(self.spark, {"source_df": source_df})
        final_df = result["final_df"]
        
        # Verify the results
        self.assertEqual(final_df.count(), 3)
        
        # Convert to pandas for easier assertions
        pdf = final_df.toPandas()
        
        # Check trimming of names
        self.assertEqual(pdf.iloc[0]["TTDBC_LASTNAME"], "Yamada")
        self.assertEqual(pdf.iloc[0]["TTDBC_FIRSTNAME"], "Taro")
        self.assertEqual(pdf.iloc[0]["TTDBC_LASTNAME_KANJI"], "山田")
        self.assertEqual(pdf.iloc[0]["TTDBC_FIRSTNAME_KANJI"], "太郎")
        
        # Check null handling for Kanji names
        self.assertEqual(pdf.iloc[1]["TTDBC_LASTNAME_KANJI"], "")
        self.assertEqual(pdf.iloc[1]["TTDBC_FIRSTNAME_KANJI"], "")
        
        # Check invalid Kanji character replacement and CR/LF handling
        self.assertTrue("\r" not in pdf.iloc[2]["TTDBC_LASTNAME_KANJI"])  # CR removed
        self.assertTrue("\n" in pdf.iloc[2]["TTDBC_LASTNAME_KANJI"])      # LF retained
        self.assertTrue("\u25A0" in pdf.iloc[2]["TTDBC_FIRSTNAME_KANJI"]) # Invalid chars replaced


if __name__ == "__main__":
    unittest.main()