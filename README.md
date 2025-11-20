# Customer Order Data Processing Pipeline

This project implements a data processing pipeline using Databricks Delta Live Tables to transform customer and order data. The pipeline reads source CSV data, performs transformations, and loads the data into Delta tables with SCD Type 2 implementation for tracking historical changes.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from specified Databricks volumes
2. Cleans the data by removing nulls and duplicates
3. Calculates total order amounts
4. Implements SCD Type 2 for tracking customer changes
5. Creates an aggregated view of customer spending

## Requirements

- Databricks Runtime 11.0 or higher
- PySpark 3.3.0 or higher
- Delta Lake 2.0.0 or higher

## Setup

1. Upload the source code to your Databricks workspace
2. Create a Delta Live Tables pipeline pointing to the main DLT script
3. Configure the pipeline with appropriate cluster settings
4. Run the pipeline

## Usage

To run the pipeline:

```bash
# Create a DLT pipeline in Databricks UI
# Point to the main module: src.dlt_pipeline
# Run the pipeline
```

## Pipeline Structure

- `bronze_customer`: Raw customer data
- `bronze_order`: Raw order data
- `silver_customer`: Cleaned customer data
- `silver_order`: Cleaned order data with calculated total amounts
- `gold_ordersummary`: SCD Type 2 table joining customer and order data
- `gold_customeraggregatespend`: Aggregated customer spending data

## Testing

Run the tests using:

```bash
python -m pytest tests/
