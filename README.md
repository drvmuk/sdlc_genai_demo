# DLT Pipeline Project

This project implements a Delta Live Tables (DLT) pipeline to load raw Customer and Order data from Unity Catalog Volumes into the Bronze layer, then transform and load the data into Silver and Gold layers using PySpark jobs.

## Setup

1. Install required packages: `pyspark`, `delta-lake`, `apache-spark-sql`, `pytest`
2. Configure SparkSession with Delta Lake and Spark SQL extensions
3. Create sample data in Unity Catalog Volumes

## Usage

1. Run the DLT pipeline using `spark-submit`: `spark-submit --class src.dlt_pipeline dlt_pipeline.py`
2. Verify data is loaded correctly into Bronze, Silver, and Gold layers

## Testing

1. Run tests using `pytest`: `pytest tests/`
2. Verify tests pass and data is loaded correctly into Bronze, Silver, and Gold layers