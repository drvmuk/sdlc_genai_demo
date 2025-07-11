# Databricks Delta Live Tables Pipeline

This project implements a data processing pipeline using Databricks and Delta Live Tables to process customer and order data.

## Overview

The pipeline performs the following operations:
1. Loads customer and order data from CSV files to Delta tables
2. Transforms order data by adding a TotalAmount column
3. Cleanses customer and order data by removing nulls and duplicates
4. Creates an ordersummary table with joined data
5. Updates the ordersummary table when customer data changes
6. Creates a customeraggregatespend table with aggregated data
7. Implements data processing using Delta Live Tables

## Setup

1. Upload the project to your Databricks workspace
2. Install required packages: `pip install -r requirements.txt`
3. Configure your Databricks cluster with the specified settings
4. Run the notebooks or jobs in the specified order

## Usage

### Running the Pipeline

1. Configure the Databricks job with the appropriate cluster settings
2. Schedule the job to run at the desired frequency
3. Monitor the job execution and logs

### Sample Commands

```python
# Load data to Delta tables
python -m src.load_data

# Transform order data
python -m src.transform_order_data

# Cleanse data
python -m src.cleanse_data

# Create ordersummary table
python -m src.create_ordersummary

# Update ordersummary table
python -m src.update_ordersummary

# Create customeraggregatespend table
python -m src.create_customeraggregatespend

# Run Delta Live Tables pipeline
python -m src.dlt_pipeline
```

## Testing

Run tests using pytest:

```bash
pytest
