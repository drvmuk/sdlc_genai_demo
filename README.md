# Customer Order Processing with Delta Live Tables

This project implements a data processing pipeline that processes customer and order data, creates SCD Type 2 tables, and generates aggregated spending reports.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from CSV files
2. Cleans the data by removing nulls and duplicates
3. Adds a TotalAmount column to the order data
4. Creates an SCD Type 2 table combining customer and order data
5. Creates an aggregated spending table by customer and date

## Implementation

The project includes two implementations:
- **Delta Live Tables (DLT)**: For continuous data processing in Databricks
- **Batch Processing**: For traditional batch processing

## Setup

### Prerequisites
- Databricks Runtime 11.3 LTS or higher
- Access to the specified volumes and catalogs

### Installation
1. Upload the project files to your Databricks workspace
2. Install required packages:
```
%pip install -r requirements.txt
```

## Usage

### Running the DLT Pipeline

1. Create a new DLT pipeline in Databricks
2. Add the `src/dlt_pipeline.py` file as the source
3. Configure the pipeline with appropriate cluster settings
4. Start the pipeline

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
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "photon": true,
  "libraries": [
    {
      "notebook": {
        "path": "/path/to/src/dlt_pipeline"
      }
    }
  ],
  "name": "Customer Order Processing",
  "storage": "/path/to/storage",
  "target": "gen_ai_poc_databrickscoe.sdlc_wizard"
}
```

### Running the Batch Pipeline

Execute the batch pipeline as a Databricks job or notebook:

```python
%run /path/to/src/batch_pipeline
```

## Testing

Run the tests using pytest:

```
pytest tests/
```

## Project Structure

```
customer_order_processing/
│
├── src/
│   ├── __init__.py
│   ├── dlt_pipeline.py      # Delta Live Tables implementation
│   └── batch_pipeline.py    # Batch processing implementation
│
├── tests/
│   ├── test_dlt_pipeline.py
│   └── test_batch_pipeline.py
│
├── data/                    # Sample data files
│   ├── sample_customer.csv
│   └── sample_order.csv
│
├── requirements.txt
├── pyproject.toml
├── README.md
└── LICENSE
