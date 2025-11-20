# Databricks ETL Pipeline

This project implements an ETL pipeline using Databricks, PySpark, and Delta Live Tables for processing customer and order data.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from CSV files in Databricks Volumes
2. Cleans the data by removing nulls and duplicates
3. Enriches the order data with a calculated TotalAmount column
4. Creates an SCD Type 2 table for tracking customer changes
5. Aggregates spending by customer and date

## Implementation

The project includes two implementations:
1. Traditional PySpark approach (`src/etl_pipeline.py`)
2. Delta Live Tables approach (`src/dlt_pipeline.py`)

## Setup and Configuration

1. Ensure you have access to the Databricks workspace with the required volumes:
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

2. Create the catalog and schema if they don't exist:
   ```sql
   CREATE CATALOG IF NOT EXISTS gen_ai_poc_databrickscoe;
   CREATE SCHEMA IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard;
   ```

## Usage

### Traditional Pipeline

Run the traditional pipeline using:

```python
from src.etl_pipeline import run_etl_pipeline
run_etl_pipeline()
```

### Delta Live Tables Pipeline

To deploy the DLT pipeline:

1. Navigate to Workflows > Delta Live Tables in your Databricks workspace
2. Create a new pipeline with the following settings:
   - Pipeline name: Customer Order ETL Pipeline
   - Source code: /path/to/src/dlt_pipeline.py
   - Target: gen_ai_poc_databrickscoe.sdlc_wizard
   - Configuration: (leave default)

3. Click Create and then Start to run the pipeline

## Testing

Run tests using pytest:

```bash
pytest tests/
```

## Project Structure

- `src/`: Source code for the ETL pipeline
  - `etl_pipeline.py`: Traditional PySpark implementation
  - `dlt_pipeline.py`: Delta Live Tables implementation
  - `scd_helper.py`: Helper functions for SCD Type 2 implementation
- `tests/`: Unit tests
- `data/`: Sample data for testing