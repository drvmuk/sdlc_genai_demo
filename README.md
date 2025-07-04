# Databricks ETL Pipeline

This project implements an ETL pipeline for processing customer and order data in Databricks. It includes data ingestion, cleansing, joining, and loading into a Delta Lake table using SCD Type 2 logic.

## Overview

The pipeline consists of the following steps:
1. Ingest raw customer and order data from CSV files
2. Cleanse the data by removing null and duplicate records
3. Create an ordersummary table if it doesn't exist
4. Join customer and order data on CustId
5. Load the joined data into the ordersummary table using SCD Type 2 logic
6. Automate updates to the ordersummary table based on changes in customer data

## Setup

1. Upload the project to your Databricks workspace
2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```
3. Configure the Databricks cluster according to the technical requirements

## Usage

### Running the ETL Pipeline

```python
from src.etl_pipeline import run_etl_pipeline

# Run the full ETL pipeline
run_etl_pipeline()

# Or run individual steps
from src.data_ingestion import ingest_data
from src.data_cleansing import cleanse_data
from src.table_management import create_ordersummary_table
from src.data_transformation import join_customer_order_data
from src.scd_management import load_scd_type2

# Ingest data
customer_df, order_df = ingest_data()

# Cleanse data
cleansed_customer_df, cleansed_order_df = cleanse_data(customer_df, order_df)

# Create ordersummary table
create_ordersummary_table()

# Join data
joined_df = join_customer_order_data(cleansed_customer_df, cleansed_order_df)

# Load data using SCD Type 2 logic
load_scd_type2(joined_df)
```

### Running Tests

```bash
pytest tests/
```

## Project Structure

- `src/`: Source code for the ETL pipeline
- `tests/`: Unit tests
- `data/`: Sample data for testing
- `requirements.txt`: Required Python packages
- `pyproject.toml`: Project metadata
- `README.md`: Project documentation
- `LICENSE`: MIT License

## Configuration

The ETL pipeline uses the following configuration:
- Customer data path: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
- Order data path: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
- Cleansed customer data path: `/tmp/cleansed_customer_data`
- Cleansed order data path: `/tmp/cleansed_order_data`
- Joined data path: `/tmp/joined_data`
- Ordersummary table: `gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary`