# Databricks ETL Pipeline

This project implements a scalable ETL pipeline using PySpark and Delta Lake on Databricks. The pipeline processes customer and order data, performs data cleansing, and implements SCD Type 2 for tracking historical changes.

## Features

- Data ingestion from CSV files to Delta tables
- Data cleansing to remove null and duplicate records
- SCD Type 2 implementation for tracking historical changes
- Automated updates when source data changes

## Setup

1. Upload the project files to your Databricks workspace
2. Install required packages from `requirements.txt`
3. Configure the cluster settings as specified in the technical requirements
4. Run the notebooks or scripts in the following order:
   - Data Ingestion
   - Data Cleansing
   - Create Order Summary Table
   - Load Order Summary Data
   - Update Order Summary Table

## Usage

### Data Ingestion

```python
from src.data_ingestion import ingest_data

ingest_data()
```

### Data Cleansing

```python
from src.data_cleansing import cleanse_data

cleanse_data()
```

### Create Order Summary Table

```python
from src.order_summary import create_order_summary_table

create_order_summary_table()
```

### Load Order Summary Data

```python
from src.order_summary import load_order_summary_data

load_order_summary_data()
```

### Update Order Summary Table

```python
from src.order_summary import update_order_summary

update_order_summary()
```

## Testing

Run tests using pytest:

```bash
pytest tests/
