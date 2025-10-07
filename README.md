# Customer Order Analytics Pipeline

This project implements a data pipeline using Databricks Delta Live Tables to process customer and order data, create SCD Type 2 tables, and generate aggregated spending reports.

## Overview

The pipeline performs the following operations:
1. Loads customer and order data from CSV files
2. Cleans the data by removing nulls and duplicates
3. Calculates total order amounts
4. Implements SCD Type 2 pattern for tracking customer changes
5. Creates aggregated customer spending reports

## Setup

1. Upload the project to your Databricks workspace
2. Ensure the following volumes exist:
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
3. Install required packages listed in requirements.txt
4. Create the DLT pipeline using the main module

## Usage

To run the pipeline:

1. Navigate to the Delta Live Tables UI in Databricks
2. Create a new pipeline
3. Select the main module as the source
4. Configure the pipeline settings
5. Deploy and run the pipeline

## Pipeline Structure

- `src/dlt_pipeline.py`: Main Delta Live Tables pipeline definition
- `src/data_processing.py`: Core data processing functions
- `src/scd_helpers.py`: Helper functions for SCD Type 2 implementation

## Testing

Run tests using:
```
pytest tests/
