# Order Processing System

This project implements a data processing pipeline for customer and order data using PySpark and Databricks Delta Lake.

## Overview

The system performs the following operations:
- Loads customer and order data from CSV files into Delta tables
- Cleanses data by removing null and duplicate records
- Creates and maintains an order summary table (SCD Type 2)
- Updates the order summary table when customer data changes
- Creates and maintains a customer aggregate spend table

## Setup

1. Upload the project to your Databricks workspace
2. Install required dependencies from `requirements.txt`
3. Configure the cluster with Databricks Runtime 11.3.x-scala2.12

## Usage

The main modules can be run as Databricks notebooks or jobs:

```python
# Load data from CSV to Delta tables
%run ./src/data_loader

# Cleanse data
%run ./src/data_cleaner

# Create and update order summary
%run ./src/order_summary_processor

# Create and update customer aggregate spend
%run ./src/customer_aggregate_processor
```

## Testing

Run tests using pytest:

```
pytest tests/
