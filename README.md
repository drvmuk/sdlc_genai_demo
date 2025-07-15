# Delta Data Processing Pipeline

This project implements a data processing pipeline using PySpark and Delta Lake to process customer and order data. The pipeline performs data loading, cleansing, joining, and aggregation operations to create various analytical views.

## Features

- Load CSV data into Delta tables
- Data cleansing (remove nulls and duplicates)
- SCD Type 2 implementation for tracking historical changes
- Data aggregation for customer spending analysis

## Setup

1. Clone this repository to your Databricks workspace
2. Install required dependencies from `requirements.txt`
3. Configure the cluster with Databricks Runtime 10.4 LTS

## Usage

The pipeline consists of several modules that can be executed in sequence:

1. Load source data into Delta tables:
```
%run ./src/load_data
```

2. Cleanse data:
```
%run ./src/cleanse_data
```

3. Create and update order summary:
```
%run ./src/order_summary
```

4. Create customer aggregate spend:
```
%run ./src/customer_aggregate
```

## Configuration

The pipeline uses the following data locations:
- Source CSV: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata` and `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
- Target Delta tables: `gen_ai_poc_databrickscoe.sdlc_wizard.customer`, `gen_ai_poc_databrickscoe.sdlc_wizard.order`, etc.

## Testing

Run tests using pytest:
```
pytest tests/
