# E2E Policy Services - BC K2H Data Extraction

This project extracts and consolidates beneficiary change and related policy/transaction data from TXDB (SQL Server) into an Oracle staging table for downstream K2H processing and reporting.

## Overview

The data pipeline extracts data from four SQL Server source tables:
- T_TX_REQUEST_POLICY
- T_TX_BASIC
- T_TX_RELATION
- T_TX_DTL_BENEFICIARY_CHANGE

It performs data cleansing, standardization, and Kanji character normalization before loading into the Oracle staging table `STG_E2E_BC_K2H_TXDB_DATA`.

## Setup

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Configure connection parameters in the Databricks environment or update the configuration in the code.

## Usage

Run the main extraction job:

```python
dbutils.notebook.run("src/e2e_bc_k2h_extraction.py", timeout_seconds=3600)
```

Or submit as a job:

```
databricks jobs create --json-file job_config.json
```

## Testing

Run the test suite:

```
pytest tests/
```

## Business Rules

The implementation enforces several business rules:
- Only rows related to beneficiary change transactions are staged
- Each record carries policy and transaction identifiers
- Kanji name fields are validated and normalized
- Names are trimmed appropriately
- Date fields are properly cast to Oracle DATE format
- Null handling and data standardization are applied

## Monitoring and Logging

The job includes comprehensive logging to track progress, record counts, and any data quality issues encountered during processing.