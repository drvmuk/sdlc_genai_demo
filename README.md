# Customer Order Data Pipeline

This project implements a PySpark job to process customer and order data into Delta tables, with an SCD Type 2 implementation for order summaries.

## Overview

The pipeline performs the following operations:
1. Loads customer and order data from CSV files
2. Transforms and cleans the data (removes nulls and duplicates)
3. Creates Delta tables for customers and orders
4. Implements an SCD Type 2 table for order summaries

## Setup

### Prerequisites
- Databricks Runtime 10.4.x-scala2.12
- Delta Lake library
- PySpark

### Installation
```bash
pip install -r requirements.txt
```

## Usage

To run the pipeline:
```bash
python -m src.order_pipeline
```

For testing:
```bash
pytest tests/
```

## Configuration

The pipeline uses the following data paths:
- Customer data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
- Order data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
- Output tables in catalog: `gen_ai_poc_databrickscoe.sdlc_wizard`