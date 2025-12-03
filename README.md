# Customer Order Processing

This project processes customer and order data using Delta Live Tables in Databricks. It implements data cleaning, transformation, SCD Type 2 table maintenance, and aggregation logic.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from CSV files
2. Cleans the data by removing nulls and duplicates
3. Calculates TotalAmount for orders
4. Creates and maintains an SCD Type 2 table combining customer and order data
5. Creates an aggregate table with customer spending by date

## Setup

### Prerequisites
- Databricks Runtime 11.3 LTS or higher with Delta Lake
- Access to the specified Volumes paths

### Installation
1. Upload the project files to your Databricks workspace
2. Install required dependencies:
```
pip install -r requirements.txt
```

## Usage

### Running as Delta Live Tables Pipeline

1. Create a new DLT pipeline in Databricks
2. Add the `src/delta_live_tables.py` file as the source
3. Configure the pipeline with appropriate cluster settings
4. Run the pipeline

Example DLT configuration:
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
  "continuous": false,
  "channel": "preview",
  "photon": true,
  "libraries": [
    {
      "notebook": {
        "path": "/path/to/src/delta_live_tables"
      }
    }
  ],
  "target": "gen_ai_poc_databrickscoe.sdlc_wizard"
}
```

### Running as Standalone Job

You can also run the processing as a standalone job:

```python
from src.standalone_processing import process_customer_order_data
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomerOrderProcessing").getOrCreate()
process_customer_order_data(spark)
```

## Testing

Run the tests using pytest:

```
pytest tests/
```

## Project Structure

- `src/delta_live_tables.py`: Main DLT pipeline implementation
- `src/scd_type2_update_logic.py`: Logic for SCD Type 2 table updates
- `src/standalone_processing.py`: Standalone version of the processing logic
- `tests/`: Test files for data processing logic
- `data/`: Sample data files for testing