from databricks import sql

def create_catalog_schema_table(catalog_name, schema_name, table_name, spark):
    # Check and create catalog
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    except Exception as e:
        print(f"Error creating catalog: {e}")
        
    # Check and create schema
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
    except Exception as e:
        print(f"Error creating schema: {e}")
        
    # Check and create table (assuming Delta format)
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} USING DELTA")
    except Exception as e:
        print(f"Error creating table: {e}")