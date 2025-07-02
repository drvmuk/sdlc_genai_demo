# Order Processing Pipeline

A production-grade PySpark data pipeline for processing customer and order data, loading them into Delta tables, and generating an order summary table using SCD Type 2 methodology.

## Overview

This project implements a data pipeline that:

1. Reads customer and order data from CSV sources
2. Cleans the data by removing null values and duplicates
3. Loads the data into Delta tables
4. Joins customer and order data
5. Creates and updates an order summary table using SCD Type 2 methodology

## Requirements

- Python 3.8+
- Apache Spark 3.3.0+
- Delta Lake 2.2.0+
- Databricks Runtime 10.4 LTS or higher

## Project Structure

```
order-processing-pipeline/
│
├── src/
│   ├── __init__.py
│   └── order_processing.py     # Main processing logic
│
├── tests/
│   └── test_order_processing.py # Unit tests
│
├── requirements.txt            # Dependencies
├── pyproject.toml             # Project metadata
├── README.md                  # This file
└── LICENSE                    # License information
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-org/order-processing-pipeline.git
cd order-processing-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Running on Databricks

1. Upload the project files to your Databricks workspace
2. Create a new job with the following configuration:
   - Cluster: Use the "Databricks Cluster for Data Ingestion" or equivalent
   - Task: Python task pointing to `src/order_processing.py`

### Running Locally (for testing)

```bash
python -m src.order_processing
```

### Running Tests

```bash
pytest
```

## Configuration

The pipeline is configured to use the following paths:

- Source data:
  - Customer data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
  - Order data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

- Target tables:
  - Customer table: `gen_ai_poc_databrickscoe.sdlc_wizard.customer`
  - Order table: `gen_ai_poc_databrickscoe.sdlc_wizard.order`
  - Order summary table: `gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary`

## License

MIT