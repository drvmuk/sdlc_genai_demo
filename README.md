# E2E Address Change YUYU Creation

This project implements the E2E Address Change YUYU Creation workflow in PySpark, replacing the legacy Informatica PowerCenter implementation. The workflow processes address change data from a staging table, performs lookups to enrich the data, and produces an outbound flat file for downstream systems.

## Overview

The workflow performs the following key steps:
1. Check if there are records to process in the source staging table
2. If records exist, process them to create the outbound YUYU file
3. Update the staging table with processing status and audit information
4. Handle trigger file creation and cleanup operations

## Setup

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Configure the environment variables in your Databricks cluster or local environment.

## Usage

Run the main workflow:
```
python -m src.main
```

## Configuration

The workflow uses the following environment variables:
- `DB_CONNECTION_E2E_ORA_STG` - Connection string for Oracle staging database
- `DB_CONNECTION_E2E_ORA_ODS` - Connection string for Oracle ODS database
- `OUTPUT_FILE_YUYU_NEW` - Path for the output YUYU file
- `TARGET_FILE_DIR` - Directory for target files
- `BAD_FILE_DIR` - Directory for rejected records
- `PROCESS_USERID` - User ID for audit purposes
- `SHELL_DIR` - Directory containing shell scripts

## Project Structure

- `src/main.py` - Main workflow orchestration
- `src/source_count_check.py` - Source record count check logic
- `src/yuyu_creation.py` - YUYU file creation and staging table update logic
- `src/utils.py` - Utility functions for file operations and common transformations
- `tests/` - Unit tests for the workflow components