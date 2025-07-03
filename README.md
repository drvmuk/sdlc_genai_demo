# Customer Order Delta Pipeline

A production-quality PySpark data pipeline for processing customer and order data into Delta tables, with SCD Type 2 implementation for tracking historical changes.

## Overview

This project implements a data pipeline that:

1. Loads customer and order data from CSV files
2. Performs data cleansing (removing nulls and duplicates)
3. Creates Delta tables for customer and order data
4. Joins the data and implements SCD Type 2 for historical tracking in a summary table

## Requirements

- Python 3.8+
- PySpark 3.3.0+
- Delta Lake 2.1.0+
- Databricks Runtime 10.4.x

## Project Structure

```
customer_order_delta_pipeline/
│
├── src/
│   ├── __init__.py
│   └── delta_pipeline.py
│
├── tests/
│   ├── __init__.py
│   └── test_delta_pipeline.py
│
├── data/
│   ├── sample_customer.csv
│   └── sample_order.csv
│
├── requirements.txt
├── pyproject.toml
├── README.md
└── LICENSE
```

## Setup

1. Clone the repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Running in Databricks

1. Upload the project to Databricks workspace
2. Create a job with the following parameters:
   - Task: Python
   - Main Python file: `src/delta_pipeline.py`
   - Cluster: Data Engineering Cluster (as specified in requirements)

### Running Tests

```bash
pytest
```

### Sample Command

To run the pipeline locally (for development):

```bash
python -m src.delta_pipeline
```

## Pipeline Configuration

The pipeline is configured to:

- Read customer data from `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
- Read order data from `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
- Write cleaned customer data to `gen_ai_poc_databrickscoe.sdlc_wizard.customer`
- Write cleaned order data to `gen_ai_poc_databrickscoe.sdlc_wizard.order`
- Create a summary table with SCD Type 2 at `gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary`

## License

MIT License