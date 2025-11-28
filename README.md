# Customer Order Processing

This project implements a data processing pipeline using Databricks Delta Live Tables to process customer and order data. The pipeline reads data from CSV files, performs data cleaning, joins the datasets, and creates SCD Type 2 tables for tracking historical changes.

## Features

- CSV data ingestion from Databricks Volumes
- Data cleaning (removing nulls and duplicates)
- SCD Type 2 implementation for tracking historical changes
- Aggregation of customer spending data
- Implementation using both batch processing and Delta Live Tables

## Project Structure

- `src/delta_live_tables.py`: Contains the Delta Live Tables implementation
- `src/batch_processing.py`: Contains the batch processing implementation
- `tests/`: Contains unit tests for the project
- `data/`: Contains sample data files for testing

## Setup

1. Upload the project to your Databricks workspace
2. Create the required Volumes:
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
3. Upload sample data to these locations

## Usage

### Running the Delta Live Tables Pipeline

1. Create a new DLT pipeline in Databricks
2. Set the source to the `src/delta_live_tables.py` file
3. Configure the pipeline settings and run

Example configuration:
```json
{
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": 1,
        "max_workers": 5
      }
    }
  ],
  "development": true,
  "continuous": false
}
```

### Running the Batch Processing Script

Execute the `src/batch_processing.py` file in a Databricks notebook or job.

Example:
```python
%run /path/to/src/batch_processing.py
```

## Testing

Run the tests using pytest:

```bash
pytest tests/
```

## License

See the LICENSE file for details.