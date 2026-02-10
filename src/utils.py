import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def get_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("E2E_AC_TXDBH_DP_YUYU_CREATION") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .getOrCreate()


def read_oracle_table(spark, connection_string, table_name, schema_name, query=None):
    """Read data from Oracle table using JDBC."""
    # In production, use Databricks secrets for credentials
    username = spark.conf.get("spark.databricks.secrets.get", "e2e_oracle:stg_username")
    password = spark.conf.get("spark.databricks.secrets.get", "e2e_oracle:stg_password")
    
    jdbc_url = connection_string
    
    if query:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({query})") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
    else:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{schema_name}.{table_name}") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
    
    return df


def write_to_oracle_table(df, connection_string, table_name, schema_name, mode="append"):
    """Write DataFrame to Oracle table using JDBC."""
    # In production, use Databricks secrets for credentials
    username = df.sparkSession.conf.get("spark.databricks.secrets.get", "e2e_oracle:stg_username")
    password = df.sparkSession.conf.get("spark.databricks.secrets.get", "e2e_oracle:stg_password")
    
    jdbc_url = connection_string
    
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"{schema_name}.{table_name}") \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .mode(mode) \
        .save()


def create_trigger_file(output_dir, trigger_file, content=""):
    """Create or update trigger file with specified content."""
    file_path = os.path.join(output_dir, trigger_file)
    with open(file_path, "w") as f:
        f.write(content)
    return file_path


def run_cleanup_script(shell_dir, script_name):
    """Run cleanup shell script."""
    script_path = os.path.join(shell_dir, script_name)
    try:
        subprocess.run([script_path], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running cleanup script: {e}")
        return False


def write_count_file(spark, count, output_dir, output_file):
    """Write source record count to output file."""
    count_df = spark.createDataFrame([(count,)], ["SOURCE_RECT"])
    output_path = os.path.join(output_dir, output_file)
    
    count_df.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(output_path)
    
    return count