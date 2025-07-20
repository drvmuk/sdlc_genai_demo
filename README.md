# Databricks Data Processing Pipeline

This project implements a data processing pipeline for customer and order data using PySpark on Databricks.

## Overview

The pipeline consists of four main components:

1. **Data Ingestion and Processing**: Reads customer and order data, applies data cleansing rules, and removes duplicate records.
2. **Data Transformation and Enrichment**: Applies SCD Type 2 logic to customer data and performs aggregation on order data.
3. **Target Table Creation and Data Loading**: Creates target tables and loads processed data into them.
4. **Incremental Load and Change Detection**: Supports incremental load and change detection for customer and order data.

## Setup

1. Upload the project to your Databricks workspace.
2. Install the required packages listed in `requirements.txt`.
3. Configure the cluster as specified in the technical requirements.

## Usage

Run the pipeline using the following commands:

```python
# Run data ingestion and processing
%run ./src/data_ingestion

# Run data transformation and enrichment
%run ./src/data_transformation

# Run target table creation and data loading
%run ./src/target_loading

# Run incremental load and change detection
%run ./src/incremental_load
```

## Configuration

The pipeline uses the following data paths:

- Customer Data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
- Order Data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
- Order Summary: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/ordersummary`
- Customer Aggregate Spend: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customeraggregatespend`

## Testing

Run tests using pytest:

```bash
pytest
