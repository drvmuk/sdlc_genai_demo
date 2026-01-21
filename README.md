# Customer Order Processing Pipeline

This project implements a data processing pipeline using Delta Live Tables in Databricks to process customer and order data, maintain SCD Type 2 history, and calculate aggregated customer spending.

## Overview

The pipeline performs the following operations:

1. Reads customer and order data from CSV files in Databricks Volumes
2. Cleans the data by removing null values and duplicates
3. Calculates TotalAmount for each order
4. Joins customer and order data
5. Implements SCD Type 2 for tracking historical changes in customer data
6. Aggregates customer spending by name and date

## Project Structure

- `src/delta_live_tables.py`: Contains the Delta Live Tables implementation
- `tests/`: Contains unit tests for the pipeline
- `data/`: Sample data files for testing

## Requirements

- Databricks Runtime 11.3 LTS or higher
- Delta Lake
- PySpark 3.4.0 or higher

## Setup

1. Upload the project files to your Databricks workspace
2. Create a Delta Live Tables pipeline using the `src/delta_live_tables.py` file
3. Configure the pipeline with the appropriate target catalog and schema

## Pipeline Configuration

When creating the Delta Live Tables pipeline in Databricks, use the following configuration:

- **Pipeline Name**: Customer Order Processing
- **Product Edition**: Advanced
- **Pipeline Mode**: Triggered
- **Cluster Mode**: Fixed Size
- **Workers**: 2-4 (adjust based on data volume)
- **Photon Acceleration**: Enabled
- **Target**: `gen_ai_poc_databrickscoe.sdlc_wizard`

## Usage

### Running the Pipeline

To run the pipeline:

1. Navigate to the Delta Live Tables UI in Databricks
2. Select your pipeline
3. Click "Start" to run the pipeline

### Sample Commands

To run the tests locally:

```bash
pytest tests/test_delta_live_tables.py
```

To run the pipeline programmatically:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines

w = WorkspaceClient()
pipeline_id = "your-pipeline-id"

# Start a pipeline update
update = w.pipelines.start_update(
    pipeline_id=pipeline_id,
    full_refresh=False  # Set to True for a full refresh
)
print(f"Started update: {update.update_id}")
```

## Tables Created

1. `customer`: Raw customer data with nulls and duplicates removed
2. `order`: Raw order data with nulls and duplicates removed, and TotalAmount calculated
3. `ordersummary`: SCD Type 2 table containing joined customer and order data with history tracking
4. `customeraggregatespend`: Aggregated customer spending by name and date

## License

This project is licensed under the MIT License - see the LICENSE file for details.