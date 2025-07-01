# Databricks ETL Pipeline

This project implements a comprehensive ETL pipeline using Databricks Delta Live Tables (DLT) following the medallion architecture (Bronze, Silver, Gold layers).

## Overview

The pipeline processes customer and order data through the following stages:
1. **Bronze Layer**: Raw data ingestion from CSV files
2. **Silver Layer**: Data cleansing, transformation, and joining
3. **Gold Layer**: Aggregation and business metrics calculation

## Setup

1. Create the necessary Unity Catalog objects (catalogs, schemas)
2. Configure Databricks clusters as specified in the requirements
3. Deploy the DLT pipelines

## Usage

### Running the ETL Pipeline

```bash
# Deploy the DLT pipeline
databricks pipelines create --settings pipeline_config.json

# Run the ETL pipeline
databricks pipelines start --pipeline-id <pipeline_id>
```

### Managing Unity Catalog Objects

```bash
# Run the Unity Catalog setup script
python src/unity_catalog_manager.py
```

## Project Structure

- `src/`: Source code for the ETL pipeline
  - `bronze_layer.py`: Bronze layer implementation
  - `silver_layer.py`: Silver layer implementation
  - `gold_layer.py`: Gold layer implementation
  - `unity_catalog_manager.py`: Unity Catalog management
  - `streaming_ingestion.py`: Real-time streaming ingestion

- `tests/`: Test cases for the ETL pipeline
  - `test_bronze_layer.py`: Tests for bronze layer
  - `test_silver_layer.py`: Tests for silver layer
  - `test_gold_layer.py`: Tests for gold layer