# Databricks DLT Pipeline

This project implements a Delta Live Tables (DLT) pipeline for processing customer and order data in Databricks.

## Overview

The pipeline performs the following operations:
1. Load customer and order data from CSV files into Delta tables
2. Transform order data by adding a TotalAmount column
3. Cleanse customer and order data by removing nulls and duplicates
4. Create an ordersummary table by joining customer and order data
5. Implement SCD Type 2 logic for the ordersummary table
6. Create a customeraggregatespend table with aggregated data
7. Implement all steps as a Delta Live Tables pipeline

## Setup

1. Upload the project to your Databricks workspace
2. Create a Databricks cluster with the following configuration:
   - Databricks Runtime Version: 10.4.x-scala2.12
   - Node Type: Standard_DS3_v2
   - Driver Node: Standard_DS3_v2
   - Worker Nodes: 2-4 Standard_DS3_v2
   - Autoscaling: Enabled
   - Auto Termination: 30 minutes
   - Libraries: delta-lake, pyspark

3. Set up the source data paths:
   - Customer data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - Order data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

## Usage

To run the DLT pipeline:

1. Create a Delta Live Tables pipeline in the Databricks workspace
2. Set the source as the `src/dlt_pipeline.py` file
3. Configure the pipeline settings as needed
4. Start the pipeline

## Testing

Run the tests using pytest:

```bash
pytest tests/
```

## Directory Structure

```
databricks_dlt_pipeline/
│
├── src/
│   ├── __init__.py
│   ├── data_loader.py
│   ├── data_transformer.py
│   ├── data_cleaner.py
│   ├── scd_handler.py
│   └── dlt_pipeline.py
│
├── tests/
│   ├── test_data_loader.py
│   ├── test_data_transformer.py
│   ├── test_data_cleaner.py
│   └── test_scd_handler.py
│
├── data/
│   ├── sample_customer_data.csv
│   └── sample_order_data.csv
│
├── requirements.txt
├── pyproject.toml
├── README.md
├── LICENSE
└── .gitignore
