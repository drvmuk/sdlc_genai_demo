# Customer Order Data Processing

This project implements a data processing pipeline using Delta Live Tables in Databricks to process customer and order data. The pipeline reads source CSV data, performs transformations, and creates SCD Type 2 tables for tracking historical changes.

## Features

- Load customer and order data from CSV files
- Data cleaning (remove nulls and duplicates)
- Calculate order total amounts
- Implement SCD Type 2 for customer order history
- Aggregate customer spending data
- Full implementation using Delta Live Tables

## Setup

1. Upload the project to your Databricks workspace
2. Configure the necessary volume paths for source data
3. Create the Delta Live Tables pipeline using the main DLT script

## Usage

1. Create a Delta Live Tables pipeline in Databricks
2. Add the main DLT script as the source
3. Configure the pipeline with appropriate cluster settings
4. Run the pipeline

## Pipeline Structure

The pipeline consists of the following tables:
- Bronze: Raw customer and order data
- Silver: Cleaned customer and order data
- Gold: SCD Type 2 order summary and customer aggregate spend

## Sample Commands

```python
# To run the pipeline manually
dbutils.notebook.run("src/customer_order_dlt", 0)
```

## Testing

Run the tests using:

```bash
pytest tests/
