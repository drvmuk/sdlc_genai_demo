from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
import datetime
import decimal

def create_sample_data(spark):
    """
    Create sample data for testing the R4B Acquisition Contract job
    """
    # Create R4B_SUB_ACQUISITION_FACT_STG sample data
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
        ("L002", "B002", "S002", datetime.date(2023, 1, 2), "Plan B", "