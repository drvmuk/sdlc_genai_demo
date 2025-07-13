# Delta Live Tables Pipeline

This project implements a data processing pipeline using Delta Live Tables to process customer and order data.

## Overview

The pipeline performs the following operations:
1. Loads customer and order data from CSV files into Delta tables
2. Implements SCD Type 2 logic to track changes in customer attributes
3. Aggregates spend data by customer and date

## Setup

1. Upload the code to your Databricks workspace
2. Create a Delta Live Tables pipeline using the main module
3. Configure the pipeline with appropriate cluster settings

## Usage

### Running the Pipeline

Create a DLT pipeline in Databricks with the following configuration:

- Cluster: Data Processing Cluster
- Databricks Runtime: 14.2.x-scala2.12
- Node Type: Standard_DS3_v2
- Autoscaling: 2-5 workers
- Libraries: Delta Lake, PySpark

### Sample Command

```python
dbutils.widgets.text("source_customer_path", "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
dbutils.widgets.text("source_order_path", "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
```

## Testing

Run tests using pytest:

```bash
pytest
