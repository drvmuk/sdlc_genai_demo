# Customer Order Data Pipeline

This project implements a data pipeline using Databricks Delta Live Tables to process customer and order data. The pipeline reads source CSV files, performs transformations, and creates SCD Type 2 tables for tracking historical changes.

## Features

- Load customer and order data from CSV files
- Data cleaning (removing nulls and duplicates)
- Calculate order total amounts
- Implement SCD Type 2 for customer data changes
- Aggregate customer spending by date
- Complete implementation using Delta Live Tables

## Setup

1. Ensure you have access to the Databricks workspace with the appropriate permissions
2. Create the required volume paths:
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
3. Upload sample customer and order CSV data to the respective volume paths
4. Install the required dependencies using `pip install -r requirements.txt`

## Usage

1. Create a Delta Live Tables pipeline in your Databricks workspace
2. Set the pipeline source to the main module: `src.dlt_pipeline`
3. Configure the pipeline with appropriate cluster settings
4. Run the pipeline to process the data

## Pipeline Structure

- **Source Data**: Customer and order CSV files
- **Bronze Tables**: Raw customer and order data
- **Silver Tables**: Cleaned customer and order data with calculated fields
- **Gold Tables**: 
  - `ordersummary`: SCD Type 2 table joining customer and order data
  - `customeraggregatespend`: Aggregated spending by customer and date

## Sample Commands

To run the tests:
```bash
python -m pytest tests/
```

To manually run the pipeline (non-DLT mode for testing):
```bash
python src/manual_pipeline.py
