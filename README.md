# SCD Type 2 Data Pipeline

This project implements a Databricks PySpark pipeline for loading customer and order data into SCD Type 2 tables. The pipeline handles data extraction from CSV files, transformation, and loading into Delta tables with SCD Type 2 tracking.

## Features

- Load customer and order data from CSV files into Delta tables
- Clean data by removing null and duplicate records
- Join customer and order data
- Implement SCD Type 2 pattern for tracking historical changes
- Update SCD Type 2 tables when source data changes

## Setup

1. Upload the project to your Databricks workspace
2. Install required dependencies from `requirements.txt`
3. Configure the cluster as specified in the requirements

## Usage

The pipeline can be executed as a Databricks job or notebook:

```bash
# Run the initial load pipeline
python -m src.scd_type2_loader

# Run the SCD Type 2 update pipeline
python -m src.scd_type2_updater
```

## Configuration

The pipeline uses the following data locations:
- Source customer data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
- Source order data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
- Target Delta tables: `gen_ai_poc_databrickscoe.sdlc_wizard` catalog/schema