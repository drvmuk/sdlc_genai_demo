# Customer Order Processing Pipeline

This project implements a data processing pipeline using Delta Live Tables in Databricks to process customer and order data. The pipeline performs data cleaning, transformation, and implements SCD Type 2 for tracking historical changes in customer data.

## Features

- Data ingestion from CSV files
- Data cleaning (removing nulls and duplicates)
- Calculation of derived fields
- SCD Type 2 implementation for historical tracking
- Data aggregation for customer spending analysis

## Setup

1. Ensure you have access to a Databricks workspace
2. Create the required volumes and catalogs:
   - Volume: `gen_ai_poc_databrickscoe/sdlc_wizard`
   - Catalog: `gen_ai_poc_databrickscoe`
   - Schema: `sdlc_wizard`

3. Upload the sample data to the appropriate volume paths:
   - Customer data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - Order data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

## Usage

### Running the Delta Live Tables Pipeline

1. Create a new DLT pipeline in Databricks
2. Add the main module as the source: `src.dlt_pipeline`
3. Configure the pipeline with appropriate cluster settings
4. Run the pipeline

## Pipeline Structure

The pipeline consists of the following steps:

1. Read and clean customer and order data
2. Calculate total amount for orders
3. Implement SCD Type 2 for the order summary table
4. Aggregate customer spending data

## Testing

Run tests using pytest:

```bash
pytest tests/
