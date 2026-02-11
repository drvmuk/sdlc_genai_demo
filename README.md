# BNCPLS IF23B 10K File Generation

This project implements a PySpark-based solution for generating "10K" flat files from Oracle source tables. The solution follows the source-to-target functional requirements for the BNCPLS – IF23B 10K File Generation project.

## Overview

The system processes data from Oracle tables (T_BENEFICIARY and T_APPLICATION_STAGING), applies transformations including AURA payload parsing via a Java UDF, calculates field values through a series of transformations, and generates flat files with approximately 10,000 records each.

## Features

- Oracle source data extraction with parameterized SQL
- AURA payload parsing and canonicalization (base64 decoding, JSON extraction, XML processing)
- Field calculations based on business rules
- File name generation with configurable patterns
- Transaction control to split output into multiple ~10K record files
- Error handling and logging

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure environment variables:
   - `ORACLE_CONNECTION_STRING`: Oracle database connection string
   - `XSLT_FILE_PATH`: Path to AURA 14 XSLT file
   - `AURA15_XSLT_FILE_PATH`: Path to AURA 15 XSLT file
   - `OUTPUT_DIR`: Directory for output files
   - `MAX_ROWS_PER_FILE`: Maximum rows per file (default: 10000)

## Usage

Run the main processing job:

```bash
spark-submit src/main.py \
  --src_sql "SELECT * FROM ZSYSBNCPLSDEV.T_BENEFICIARY b JOIN ZSYSBNCETLDEV.T_APPLICATION_STAGING a ON b.POLICY_NUMBER = a.POLICY_NUMBER WHERE a.PROCESS_DATE = '2023-10-01'" \
  --xslt_file "/path/to/aura14.xslt" \
  --aura15_xslt_file "/path/to/aura15.xslt" \
  --output_dir "/output/path" \
  --batch_id "BATCH001" \
  --run_id "RUN123"
```

## Testing

Run the test suite:

```bash
pytest tests/
