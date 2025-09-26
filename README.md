# Customer Order Processing Pipeline

This project implements a data processing pipeline for customer and order data using Databricks Delta Live Tables.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from source volumes
2. Cleans the data by removing nulls and duplicates
3. Calculates total amount for each order
4. Implements SCD Type 2 pattern for tracking customer changes
5. Aggregates customer spending by date

## Architecture

The solution is implemented in two ways:
1. Standard PySpark processing in `data_processing.py`
2. Delta Live Tables pipeline in `dlt_pipeline.py`

## Setup

### Prerequisites
- Databricks Runtime 11.3 LTS or higher
- Access to the specified volumes

### Installation
1. Upload the project files to your Databricks workspace
2. Install required dependencies:
```
pip install -r requirements.txt
```

## Usage

### Running the Standard Pipeline

```python
from src.data_processing import main

main()
```

### Running the DLT Pipeline

Create a new DLT pipeline in the Databricks UI with the following settings:
- Pipeline name: `Customer_Order_Processing`
- Source file: `/path/to/src/dlt_pipeline.py`
- Target schema: `gen_ai_poc_databrickscoe.sdlc_wizard`
- Cluster mode: `Enhanced`

Then click "Create" and "Start" to run the pipeline.

## Testing

Run the tests using pytest:

```
pytest tests/
```

## Data Flow

1. Bronze Layer: Raw data from CSV files
2. Silver Layer: Cleaned data with business transformations
3. Gold Layer: Aggregated data for analytics

## Tables Created

- `ordersummary`: SCD Type 2 table with customer and order data
- `customeraggregatespend`: Aggregated spending by customer and date