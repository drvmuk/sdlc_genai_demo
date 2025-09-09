# Databricks Data Processing Pipeline

This project implements a data processing pipeline using Databricks and Delta Live Tables. The pipeline processes customer and order data, creates derived tables, and implements SCD Type 2 logic for tracking historical changes.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from CSV files
2. Cleans the data (removes nulls and duplicates)
3. Calculates order total amounts
4. Creates an SCD Type 2 table to track customer changes
5. Aggregates customer spending data

## Setup

1. Upload the code to your Databricks workspace
2. Configure the volumes to point to your data sources
3. Create the necessary catalogs and schemas in your Databricks environment

## Usage

### Running the Standard Pipeline

```python
dbutils.notebook.run("src/data_processing.py", timeout_seconds=600)
```

### Running the Delta Live Tables Pipeline

1. Create a DLT pipeline in the Databricks UI
2. Add the `src/dlt_pipeline.py` notebook as the source
3. Configure the pipeline settings and run

## Testing

Run the tests using pytest:

```
pytest tests/
