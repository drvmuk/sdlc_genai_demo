# Customer Order Analytics Pipeline

This project implements a data processing pipeline using Databricks Delta Live Tables (DLT) to process customer and order data, create SCD Type 2 tables for tracking historical changes, and generate aggregated spending reports.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from volumes
2. Cleans and transforms the data (removing nulls, duplicates)
3. Calculates order total amounts
4. Implements SCD Type 2 tracking for customer changes
5. Creates aggregate spending reports by customer and date

## Setup

1. Ensure you have access to the Databricks workspace with the required volumes:
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

2. Create the catalog and schema if they don't exist:
   ```sql
   CREATE CATALOG IF NOT EXISTS gen_ai_poc_databrickscoe;
   CREATE SCHEMA IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard;
   ```

3. Install required packages:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Running the DLT Pipeline

1. Create a Delta Live Tables pipeline in your Databricks workspace
2. Set the source to the `src/dlt_pipeline.py` file
3. Configure the pipeline with appropriate cluster settings
4. Run the pipeline

### Sample Commands

To run the non-DLT version for testing:
```
python src/batch_pipeline.py
```

To run tests:
```
pytest tests/
```

## Tables Created

- `gen_ai_poc_databrickscoe.sdlc_wizard.customer` - Bronze layer customer data
- `gen_ai_poc_databrickscoe.sdlc_wizard.order` - Bronze layer order data
- `gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary` - SCD Type 2 table with customer and order data
- `gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend` - Aggregated spending by customer and date