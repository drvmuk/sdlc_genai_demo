# Customer Order Processing Pipeline

This project implements a data processing pipeline using Databricks Delta Live Tables to process customer and order data. The pipeline reads source CSV data, performs transformations, and loads the data into various Delta tables with SCD Type 2 implementation for tracking historical changes.

## Features

- Reads customer and order data from CSV files
- Performs data cleansing (removes nulls and duplicates)
- Calculates total order amounts
- Implements SCD Type 2 for tracking historical changes in customer data
- Aggregates customer spending by name and date

## Setup

1. Upload the project to your Databricks workspace
2. Create the necessary volumes and upload source data
3. Create the Delta Live Tables pipeline using the provided DLT script

## Usage

To run the pipeline:

1. Navigate to Workflows > Delta Live Tables in your Databricks workspace
2. Create a new pipeline
3. Add the main DLT script (`src/customer_order_pipeline.py`) as the source
4. Configure the pipeline settings and run

## Requirements

- Databricks Runtime 11.0 or higher
- Delta Lake
- PySpark