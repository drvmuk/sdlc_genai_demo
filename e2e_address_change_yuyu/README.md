# E2E Address Change YUYU Processing

This project implements a Databricks/PySpark data pipeline that processes address change records from Oracle staging tables, creates a downstream extract file, and updates the staging control/audit attributes for processed records.

## Overview

The pipeline performs the following key functions:

1. Reads address change candidate records from Oracle staging source `STG_E2E_AC_TXDBH_DATA`
2. Enriches data with policy owner information from lookup tables
3. Applies business rules for formatting addresses, postal codes, phone numbers, etc.
4. Creates a flat-file style target dataset `DPAddressChangeYUYU`
5. Updates the staging table with processing metadata

## Setup

1. Configure the database connection parameters in `src/config.py`
2. Install dependencies: `pip install -r requirements.txt`
3. Run the main processing job: `python -m src.process_address_changes`

## Testing

Run the test suite with:
```
pytest
```

## Configuration

The following environment variables can be set:
- `JDBC_URL`: JDBC connection URL for Oracle database
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password
- `PROCESS_USERID`: User ID to stamp in process records

## Directory Structure

- `src/`: Source code for the data processing pipeline
- `tests/`: Unit and integration tests
- `data/`: Sample data for testing