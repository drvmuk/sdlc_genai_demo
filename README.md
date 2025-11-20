# Customer Order Processing Pipeline

This project implements a data processing pipeline for customer and order data using Databricks, PySpark, and Delta Lake. It provides both batch processing and Delta Live Tables (DLT) implementations.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from specified volumes
2. Cleans the data by removing nulls and duplicates
3. Adds a TotalAmount column to the order data
4. Creates an SCD Type 2 table (ordersummary) by joining customer and order data
5. Creates an aggregated table (customeraggregatespend) with total spending by customer and date

## Project Structure

- `src/batch_processing.py`: Batch processing implementation
- `src/delta_live_tables.py`: Delta Live Tables implementation
- `tests/`: Unit tests for both implementations
- `data/`: Sample data files for testing

## Setup

1. Upload the project to your Databricks workspace
2. Install required dependencies:
   ```
   %pip install -r requirements.txt
   ```

## Usage

### Batch Processing

Run the batch processing pipeline using the following command:

```python
%run ./src/batch_processing.py
```

### Delta Live Tables

To use the Delta Live Tables implementation:

1. Create a new DLT pipeline in the Databricks UI
2. Add the `src/delta_live_tables.py` file as the source
3. Configure the pipeline with appropriate cluster settings
4. Run the pipeline

## Sample Commands

### Running Tests

```bash
python -m pytest tests/
```

### Examining Data

```python
# View customer data
display(spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.customer"))

# View order data
display(spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.order"))

# View ordersummary data
display(spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"))

# View customeraggregatespend data
display(spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend"))
```

## Notes

- The SCD Type 2 implementation maintains history by tracking active/