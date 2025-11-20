# Customer Order Processing

A production-quality PySpark application for processing customer and order data using Delta Live Tables in Databricks.

## Overview

This project implements a data pipeline that:
1. Reads customer and order data from CSV files
2. Cleans the data by removing nulls and duplicates
3. Calculates total order amounts
4. Implements SCD Type 2 for tracking changes in customer data
5. Creates aggregated customer spend data
6. Provides both batch processing and Delta Live Tables implementations

## Project Structure

- `src/data_processing.py`: Batch processing implementation
- `src/dlt_pipeline.py`: Delta Live Tables implementation
- `tests/`: Unit tests for the data processing logic
- `data/`: Sample data files for testing

## Setup

1. Upload the project to your Databricks workspace
2. Install required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Batch Processing

```python
from src.data_processing import run_batch_processing
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Customer Order Processing").getOrCreate()
run_batch_processing(spark)
```

### Delta Live Tables

To deploy the DLT pipeline:

1. Navigate to Workflows > Delta Live Tables in your Databricks workspace
2. Create a new pipeline
3. Set the source to `src/dlt_pipeline.py`
4. Configure the target schema as `gen_ai_poc_databrickscoe.sdlc_wizard`
5. Deploy and run the pipeline

## Testing

Run the tests using pytest:

```bash
pytest tests/
```

## Data Flow

1. Source data is read from CSV files in the specified volume paths
2. Data is cleaned (nulls and duplicates removed)
3. Total amount is calculated for orders
4. Customer and order data are joined to create the ordersummary table
5. SCD Type 2 logic tracks changes in customer data
6. Aggregated customer spend is calculated and stored