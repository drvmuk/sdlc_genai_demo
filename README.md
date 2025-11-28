# Customer Order Processing Pipeline

This project implements a data processing pipeline for customer and order data using Databricks, PySpark, and Delta Live Tables.

## Overview

The pipeline processes customer and order data from CSV files, applies transformations, and loads the data into Delta tables. It implements SCD Type 2 for tracking changes in customer data and calculates aggregate spending metrics.

## Features

- Data ingestion from CSV files
- Data cleaning (removal of nulls and duplicates)
- SCD Type 2 implementation for tracking historical changes
- Data aggregation for customer spending analysis
- Implementation using both standard PySpark and Delta Live Tables

## Project Structure

```
customer-order-processing/
│
├── src/
│   ├── __init__.py
│   ├── data_processing.py   # Standard PySpark implementation
│   └── dlt_pipeline.py      # Delta Live Tables implementation
│
├── tests/
│   ├── test_data_processing.py
│   └── test_dlt_pipeline.py
│
├── data/                    # Sample data for testing
│   ├── sample_customer.csv
│   └── sample_order.csv
│
├── requirements.txt
├── pyproject.toml
├── README.md
└── LICENSE
```

## Setup

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

2. Configure Databricks workspace connection:

```bash
databricks configure --token
```

## Usage

### Running the Standard Pipeline

To run the standard PySpark pipeline:

```python
from src.data_processing import main

main()
```

### Deploying the Delta Live Tables Pipeline

To deploy the DLT pipeline in Databricks:

1. Upload the `src/dlt_pipeline.py` file to your Databricks workspace
2. Create a new DLT pipeline with the following configuration:
   - Source: The uploaded `dlt_pipeline.py` file
   - Target: `gen_ai_poc_databrickscoe.sdlc_wizard`
   - Cluster mode: Enhanced autoscaling
3. Start the pipeline

## Sample Commands

### Running Tests

```bash
pytest tests/
```

### Running Individual Components

```python
# Process just the customer data
from src.data_processing import get_spark_session, read_source_data, clean_data

spark = get_spark_session()
customer_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
customer_df, _ = read_source_data(spark, customer_path, "")
customer_df_clean = clean_data(customer_df)
customer_df_clean.show()
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.