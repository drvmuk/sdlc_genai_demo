# SFDC Account Integration

This project implements a PySpark-based data integration pipeline to extract Salesforce Account data from `SFDC_ABS.DIM_MPE_SF_ACCOUNT` and load it into `STG_MPE_SF_ACCOUNT` staging table.

## Overview

The pipeline replicates the functionality of the Informatica PowerMart mapping "Sample1" by:
- Reading data from the Oracle source table
- Applying filters for NAMR region and incremental load window
- Performing minimal transformations (primarily field pass-through)
- Writing data to the Oracle staging table

## Features

- Direct field passthrough with 125 columns preserved
- Regional filter (ACC_PARTNER_REGION='NAMR')
- Incremental load filter based on DW_UPDATE_DT
- Data type alignment between source and target
- Support for large text fields and precision/scale for numeric fields

## Setup and Configuration

1. Install required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure connection parameters in the config section of the main module

## Usage

Run the pipeline with a specific update date parameter:

```bash
python -m sfdc_account_integration.src.sfdc_account_etl --update_dt "MM/DD/YYYY"
```

Or in a Databricks notebook:

```python
from sfdc_account_integration.src.sfdc_account_etl import extract_transform_load
extract_transform_load(spark, "MM/DD/YYYY")
```

## Testing

Run tests using pytest:

```bash
pytest sfdc_account_integration/tests/
