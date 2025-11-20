# Customer Order Processing with Delta Live Tables

This project implements a data processing pipeline using Databricks Delta Live Tables to process customer and order data, create SCD Type 2 tables, and generate customer spending aggregates.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from CSV files
2. Cleans the data by removing nulls and duplicates
3. Adds a TotalAmount column to the order data
4. Creates an SCD Type 2 table by joining customer and order data
5. Updates the SCD Type 2 table when customer data changes
6. Creates an aggregate table with customer spending by name and date

## Project Structure

- `src/data_processing.py`: Standard PySpark implementation
- `src/dlt_pipeline.py`: Delta Live Tables implementation
- `tests/`: Unit tests for the data processing logic
- `data/`: Sample data files for testing

## Setup

1. Upload the project to your Databricks workspace
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Running the Standard PySpark Implementation

```python
from src.data_processing import main

main()
```

### Running the Delta Live Tables Pipeline

1. Create a new DLT pipeline in Databricks
2. Add the `src/dlt_pipeline.py` file as the source
3. Configure the pipeline with the following settings:
   - Target: `gen_ai_poc_databrickscoe.sdlc_wizard`
   - Storage location: Your preferred DBFS location
   - Cluster mode: Enhanced autoscaling
4. Start the