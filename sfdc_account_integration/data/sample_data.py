"""
Sample data generator for testing the SFDC Account ETL pipeline.
This module creates a small dataset that mimics the structure of the source data.
"""

from datetime import datetime, timedelta
import random
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType

def generate_sample_data(spark: SparkSession, num_rows: int = 10):
    """
    Generate sample data for testing the ETL pipeline.
    
    Args:
        spark: SparkSession object
        num_rows: Number of sample rows to generate
        
    Returns:
        DataFrame with sample data
    """
    # Define schema matching a subset of the source table
    schema = StructType([
        StructField("SK_ACCOUNT_ID", DecimalType(15, 0), False),
        StructField("DW_SF_ACCOUNT_ID", DecimalType(15, 0), False),
        StructField("DW_SF_USER_ID", DecimalType(15, 0), False),
        StructField("DW_SF_CUSTOMER_TYPE_ID", DecimalType(15, 0), False),
        StructField("SOURCEKEYID", StringType(), True),
        StructField("DELETED_FLG", StringType(), True),
        StructField("ACC_NAME", StringType(), True),
        StructField("ACC_PARTNER_REGION", StringType(), True),
        StructField("ACC_ACCOUNT_NUMBER", StringType(), True),
        StructField("ACC_PROSPECT_NUMBER", StringType(), True),
        StructField("ACC_BILLING_POSTAL_CODE", DecimalType(15, 0), True),
        StructField("ACC_SHIPPING_POSTAL_CODE", DecimalType(15, 0), True),
        StructField("LATITUDE", DecimalType(18, 15), True),
        StructField("LONGITUDE", DecimalType(18, 15), True),
        StructField("COMPANY_DESCRIPTION", StringType(), True),
        StructField("TAG_ACCOUNT_AS", StringType(), True),
        StructField("ACC_OPEN_OPP_DOLLARS_CURNCY_FY", DecimalType(18, 3), True),
        StructField("SRC_ACC_CREATED_DATE", DateType(), True),
        StructField("DW_UPDATE_DT", DateType(), True),
        StructField("SUSPENDED_STATE", StringType(), True),
    ])
    
    # Generate data
    data = []
    regions = ["NAMR", "EMEA", "APAC", "LATAM"]
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(1, num_rows + 1):
        account_id = 1000000000000 + i
        region = regions[i % len(regions)]
        update_dt = base_date + timedelta(days=i % 20)
        
        row = (
            Decimal(account_id),                      # SK_ACCOUNT_ID
            Decimal(2000000000000 + i),               # DW_SF_ACCOUNT_ID
            Decimal(3000000000000 + i),               # DW_SF_USER_ID
            Decimal(random.randint(1, 5)),            # DW_SF_CUSTOMER_TYPE_ID
            f"SRC-{i:05d}",                           # SOURCEKEYID
            "N",                                       # DELETED_FLG
            f"Test Account {i}",                       # ACC_NAME
            region,                                    # ACC_PARTNER_REGION
            f"ACC{i:06d}",                             # ACC_ACCOUNT_NUMBER
            f"PROS{i:06d}",                            # ACC_PROSPECT_NUMBER
            Decimal(random.randint(10000, 99999)),     # ACC_BILLING_POSTAL_CODE
            Decimal(random.randint(10000, 99999)),     # ACC_SHIPPING_POSTAL_CODE
            Decimal(f"{random.uniform(30, 50):.15f}"), # LATITUDE
            Decimal(f"{random.uniform(-120, -70):.15f}"), # LONGITUDE
            f"This is a sample company description for account {i}. " * 5, # COMPANY_DESCRIPTION
            f"tag1,tag2,tag3,tag{i}",                  # TAG_ACCOUNT_AS
            Decimal(random.uniform(1000, 100000)),     # ACC_OPEN_OPP_DOLLARS_CURNCY_FY
            base_date - timedelta(days=random.randint(30, 365)), # SRC_ACC_CREATED_DATE
            update_dt,                                 # DW_UPDATE_DT
            "ACTIVE" if i % 5 != 0 else "SUSPENDED",   # SUSPENDED_STATE
        )
        data.append(row)
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    return df

if __name__ == "__main__":
    # This can be run directly to generate and display sample data
    spark = SparkSession.builder \
        .appName("Generate Sample Data") \
        .master("local[*]") \
        .getOrCreate()
    
    sample_df = generate_sample_data(spark, 20)
    print(f"Generated {sample_df.count()} sample rows")
    sample_df.show(5, truncate=False)
    
    # Write to CSV for inspection
    sample_df.write.mode("overwrite").csv("sample_data.csv", header=True)
    
    spark.stop()