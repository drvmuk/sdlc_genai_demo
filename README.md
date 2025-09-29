# Customer Order Data Processor

A Databricks PySpark application for processing customer and order data with SCD Type 2 implementation.

## Overview

This application processes customer and order data from CSV sources, cleans the data, and loads it into Delta tables. It implements SCD Type 2 for tracking historical changes and provides aggregated customer spending analysis.

## Features

- Data loading from CSV sources
- Data cleaning (removing nulls and duplicates)
- SCD Type 2 implementation for tracking historical changes
- Customer spend aggregation

## Setup

1. Upload the code to your Databricks workspace
2. Ensure the required volumes are accessible:
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
3. Create the necessary catalog and schema:
   ```sql
   CREATE CATALOG IF NOT EXISTS gen_ai_poc_databrickscoe;
   CREATE SCHEMA IF NOT EXISTS gen_ai_poc_databrickscoe.sdlc_wizard;
   ```

## Usage

Run the main script to process the data:

```python
%run /path/to/src/data_processor
```

Or import and call the main function:

```python
from src.data_processor import main
main()
```

## Testing

Run the tests using pytest:

```
pytest -xvs tests/
```

## Sample Data

Sample data files are provided in the `data/` directory for testing purposes.

## Directory Structure

- `src/`: Source code
- `tests/`: Unit and integration tests
- `data/`: Sample data files