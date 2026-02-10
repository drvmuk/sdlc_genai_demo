# E2E Policy Services Control

This project implements the source record count control check for E2E Policy Services data reconciliation.

## Overview

The primary purpose is to produce a control/check output containing the source record count extracted from the staging source dataset for reconciliation and audit purposes.

## Components

- Source data extraction from Oracle staging source `STG_E2E_AC_TXDBH_DATA`
- Application of source extraction SQL defined by parameter `M_SRC_SQL`
- Aggregation to derive a single value: `SOURCE_REC_COUNT`
- Output to flat file target as `SOURCE_RECT`

## Setup

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Configure connection parameters in your Databricks environment

## Usage

Run the main script with appropriate parameters:

```
python -m src.source_count_check --param-file /path/to/params.json
```

Or in a Databricks notebook:

```python
from src.source_count_check import run_source_count_check

run_source_count_check(
    m_src_sql="SELECT * FROM ZSYSE2EDEV.STG_E2E_AC_TXDBH_DATA WHERE process_date = '2023-01-01'",
    output_path="/mnt/control/source_counts/",
    output_filename="source_count_20230101.csv"
)
