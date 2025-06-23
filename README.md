# ETL Job

This project implements an ETL job to load raw Customer and Order data from Unity Catalog volumes into the Bronze layer, creating the "customer_raw" and "orders_raw" tables with SCD type-2, keeping history, and using watermark columns.

## Setup

1. Install the required packages using `poetry install`.
2. Create a SparkSession using `spark = SparkSession.builder.appName("ETL Job").getOrCreate()`.

## Usage

1. Run the ETL job using `python etl_job.py`.
2. Test the ETL job using `pytest tests/test_etl_job.py`.