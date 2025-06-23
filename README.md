# Databricks ETL Project

This project implements ETL pipelines using Databricks Delta Live Tables.

## Setup

1. Install required packages: `pip install -r requirements.txt`
2. Configure Unity Catalog API credentials

## Usage

1. Run `bronze_layer_ingestion.py` to ingest raw data into Bronze layer
2. Run `silver_layer_transformation.py` to transform data into Silver layer
3. Run `gold_layer_aggregation.py` to aggregate data into Gold layer
