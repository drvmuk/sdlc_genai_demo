# R4B Acquisition Contract Job

This project implements a PySpark job to create and populate the R4B Acquisition Contract Table by performing data transformations, joins, and applying watermarking and deletion logic as specified in TR-DRVD-001.

## Overview

The job performs the following operations:
1. Creates the schema and table if they don't exist
2. Processes data from source tables through two main flows
3. Joins and transforms the data
4. Applies watermarking and deletion logic
5. Writes the final dataset to the target table

## Setup

1. Upload the project files to your Databricks workspace
2. Install the required dependencies:
```
pip install -r requirements.txt
```

## Usage

The main job can be executed in a Databricks notebook or as a scheduled job:

```python
from src.r4b_acq_contract_job import run_r4b_acq_contract_job

run_r4b_acq_contract_job()
```

## Configuration

The job uses the following source tables:
- R4B_SUB_ACQUISITION_FACT_STG
- CONTRACT
- margin_control

And writes to:
- r4b_acq_contract (in drvd__app_r4b schema)

## Testing

Run the tests using pytest:

```
pytest tests/
```

## Logging

The job logs execution details, row counts, and any errors encountered during processing.