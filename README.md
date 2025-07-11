# DLT Customer Order Pipeline

This project implements a Delta Live Tables (DLT) pipeline for processing customer and order data according to specified technical requirements.

## Overview

The pipeline performs the following operations:
- Loads customer and order data from CSV files into Delta tables
- Joins customer and order data to create an order summary table
- Updates the order summary table when customer data changes
- Creates an aggregated customer spend table

## Setup

1. Upload the source code to your Databricks workspace
2. Create a DLT pipeline using the main module
3. Configure the pipeline to use the specified cluster configuration
4. Run the pipeline

## Usage

### Pipeline Configuration

- Cluster: Databricks Runtime 11.3.x-scala2.12
- Node Type: Standard_DS3_v2
- Worker Nodes: 2-5 (autoscaling)
- Auto Termination: 30 minutes

### Running the Pipeline

Create a DLT pipeline in Databricks and point it to the main module:

```
src.dlt_pipeline
```

## Testing

Run the tests using pytest:

```
pytest tests/
