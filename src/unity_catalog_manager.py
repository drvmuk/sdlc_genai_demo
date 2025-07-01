# Databricks notebook source
from pyspark.sql import SparkSession
import time

def create_unity_catalog_objects():
    """
    Create and manage Unity Catalog objects for the ETL pipeline.
    This includes creating catalogs, schemas, and setting up ACLs.
    """
    spark = SparkSession.builder.getOrCreate()
    
    try:
        print("Creating Unity Catalog objects...")
        
        # Create catalogs if they don't exist
        spark.sql("CREATE CATALOG IF NOT EXISTS bronzezone")
        spark.sql("CREATE CATALOG IF NOT EXISTS silverzone")
        spark.sql("CREATE CATALOG IF NOT EXISTS goldzone")
        
        # Create schemas in each catalog
        spark.sql("CREATE SCHEMA IF NOT EXISTS bronzezone.data")
        spark.sql("CREATE SCHEMA IF NOT EXISTS silverzone.data")
        spark.sql("CREATE SCHEMA IF NOT EXISTS goldzone.data")
        
        print("Unity Catalog objects created successfully.")
        
        # Set up ACLs for the catalogs and schemas
        setup_acls()
        
    except Exception as e:
        print(f"Error creating Unity Catalog objects: {str(e)}")
        raise

def setup_acls():
    """
    Set up Access Control Lists (ACLs) for Unity Catalog objects.
    """
    spark = SparkSession.builder.getOrCreate()
    
    try:
        print("Setting up ACLs...")
        
        # Grant permissions on catalogs
        spark.sql("GRANT USE CATALOG, CREATE ON CATALOG bronzezone TO `data_engineers`")
        spark.sql("GRANT USE CATALOG ON CATALOG bronzezone TO `data_analysts`")
        
        spark.sql("GRANT USE CATALOG, CREATE ON CATALOG silverzone TO `data_engineers`")
        spark.sql("GRANT USE CATALOG ON CATALOG silverzone TO `data_analysts`")
        
        spark.sql("GRANT USE CATALOG, CREATE ON CATALOG goldzone TO `data_engineers`")
        spark.sql("GRANT USE CATALOG, CREATE ON CATALOG goldzone TO `data_analysts`")
        
        # Grant permissions on schemas
        spark.sql("GRANT USE SCHEMA, CREATE ON SCHEMA bronzezone.data TO `data_engineers`")
        spark.sql("GRANT USE SCHEMA ON SCHEMA bronzezone.data TO `data_analysts`")
        
        spark.sql("GRANT USE SCHEMA, CREATE ON SCHEMA silverzone.data TO `data_engineers`")
        spark.sql("GRANT USE SCHEMA ON SCHEMA silverzone.data TO `data_analysts`")
        
        spark.sql("GRANT USE SCHEMA, CREATE ON SCHEMA goldzone.data TO `data_engineers`")
        spark.sql("GRANT USE SCHEMA, CREATE ON SCHEMA goldzone.data TO `data_analysts`")
        
        # Grant permissions on tables
        spark.sql("GRANT SELECT, MODIFY ON TABLE bronzezone.data.customer_raw TO `data_engineers`")
        spark.sql("GRANT SELECT ON TABLE bronzezone.data.customer_raw TO `data_analysts`")
        
        spark.sql("GRANT SELECT, MODIFY ON TABLE bronzezone.data.orders_raw TO `data_engineers`")
        spark.sql("GRANT SELECT ON TABLE bronzezone.data.orders_raw TO `data_analysts`")
        
        spark.sql("GRANT SELECT, MODIFY ON TABLE silverzone.data.customer_order_combined TO `data_engineers`")
        spark.sql("GRANT SELECT ON TABLE silverzone.data.customer_order_combined TO `data_analysts`")
        
        spark.sql("GRANT SELECT, MODIFY ON TABLE goldzone.data.customer_order_summary_by_age TO `data_engineers`")
        spark.sql("GRANT SELECT ON TABLE goldzone.data.customer_order_summary_by_age TO `data_analysts`")
        
        spark.sql("GRANT SELECT, MODIFY ON TABLE goldzone.data.customer_order_summary_by_domain TO `data_engineers`")
        spark.sql("GRANT SELECT ON TABLE goldzone.data.customer_order_summary_by_domain TO `data_analysts`")
        
        print("ACLs set up successfully.")
        
    except Exception as e:
        print(f"Error setting up ACLs: {str(e)}")
        raise

if __name__ == "__main__":
    create_unity_catalog_objects()