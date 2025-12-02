# Customer Order Processing Pipeline

This project implements a data processing pipeline for customer and order data using Databricks Delta Live Tables. The pipeline processes customer and order data, creates SCD Type 2 tables for tracking historical changes, and provides aggregated spending insights.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from specified volumes
2. Cleans data by removing nulls and duplicates
3. Calculates total amount for each order
4. Creates an SCD Type 2 table to track changes in customer data
5. Aggregates customer spending by name and date

## Project Structure

- `src/delta_live_tables.py`: Contains the Delta Live Tables implementation
- `src/batch_processing.py`: Contains the batch processing implementation (alternative to DLT)
- `tests/`: Contains unit tests for the pipeline components
- `data/`: Contains sample data for testing

## Setup

1. Upload the project to your Databricks workspace
2. Install required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Ensure you have access to the specified volumes:
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

## Usage

### Running the Delta Live Tables Pipeline

1. Create a new DLT pipeline in Databricks
2. Set the source to the `src/delta_live_tables.py` file
3. Configure the pipeline with appropriate cluster settings
4. Run the pipeline

### Running the Batch Processing Version

Execute the following command in a Databricks notebook:

```python
%run /path/to/src/batch_processing.py
```

## Testing

Run tests using pytest:

```
pytest tests/
```

## Sample Commands

To manually test the pipeline components:

```python
# Import the modules
from src.batch_processing import create_spark_session, read_source_data, clean_data

# Create a Spark session
spark = create_spark_session()

# Read source data
customer_df, order_df = read_source_data(spark)

# Display sample data
display(customer_df.limit(5))
display(order_df.limit(5))
