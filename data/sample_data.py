"""
Sample data generation for testing DQX rules.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import datetime

def create_sample_data(spark: SparkSession, output_path: str = None):
    """
    Create sample data for testing DQX rules.
    
    Args:
        spark: SparkSession
        output_path: Optional path to save the sample data
    
    Returns:
        Sample DataFrame
    """
    # Define schema
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("age", IntegerType(), True),
        StructField("status", StringType(), True)
    ])
    
    # Create sample data
    data = [
        (1001, "John Smith", "john.smith@example.com", datetime.date(2020, 1, 15), 35, "active"),
        (1002, "Jane Doe", "jane.doe@example.com", datetime.date(2020, 2, 20), 28, "active"),
        (1003, "Bob Johnson", "bob.johnson@example.com", datetime.date(2020, 3, 10), 42, "inactive"),
        (1004, "Alice Brown", None, datetime.date(2020, 4, 5), 31, "active"),
        (1005, "Charlie Davis", "charlie.davis@example.com", None, 45, "active"),
        (1006, "Eva Wilson", "eva.wilson@example.com", datetime.date(2020, 6, 12), None, "inactive"),
        (1007, "Frank Miller", "frank.miller@example.com", datetime.date(2020, 7, 8), 39, None),
        (1008, "Grace Taylor", "grace.taylor@example.com", datetime.date(2020, 8, 22), 27, "active"),
        (1009, "Henry Clark", "henry.clark@example.com", datetime.date(2020, 9, 18), 33, "active"),
        (1010, "Ivy Martin", "ivy.martin@example.com", datetime.date(2020, 10, 30), 29, "inactive"),
        # Duplicate customer_id for testing primary key checks
        (1010, "Duplicate User", "duplicate@example.com", datetime.date(2020, 11, 5), 50, "active")
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    
    # Save if output path is provided
    if output_path:
        df.write.format("delta").mode("overwrite").save(output_path)
        print(f"Sample data saved to {output_path}")
    
    return df

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder.appName("DQX Sample Data").getOrCreate()
    
    # Generate sample data
    df = create_sample_data(spark)
    
    # Show sample data
    df.show()