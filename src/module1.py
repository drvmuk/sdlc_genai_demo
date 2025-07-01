from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("PySpark Template").getOrCreate()

    # Read sample CSV data
    df = spark.read.csv("data/sample.csv", header=True, inferSchema=True)

    # Perform a simple transformation
    transformed_df = df.withColumn("double_value", df["value"] * 2)

    # Write the transformed data back to CSV
    transformed_df.write.csv("output.csv", header=True)

    spark.stop()

if __name__ == "__main__":
    main()