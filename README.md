# Databricks ETL Pipeline

A scalable ETL (Extract, Transform, Load) pipeline built with PySpark for Databricks environments. This project demonstrates best practices for data processing at scale.

## Features

- Data ingestion from various sources (CSV, JSON, Parquet)
- Data transformation and cleaning
- Data aggregation and analytics
- Delta Lake integration for ACID transactions
- Error handling and logging
- Unit testing with pytest

## Setup

1. Clone this repository to your Databricks workspace or local environment.

2. Install dependencies:
   ```
   pip install -e .
   ```

3. Configure your data sources in the configuration file.

## Usage

### Running the ETL Pipeline

```python
from databricks_etl_pipeline.etl import run_pipeline

# Run the complete pipeline
run_pipeline(
    source_path="dbfs:/mnt/data/raw/sales/", 
    target_path="dbfs:/mnt/data/processed/sales/",
    date="2023-10-01"
)
```

### Running Individual Components

```python
from databricks_etl_pipeline.transform import clean_and_transform_data
from databricks_etl_pipeline.aggregate import create_sales_summary

# Transform data
transformed_df = clean_and_transform_data(raw_df)

# Create aggregations
summary_df = create_sales_summary(transformed_df)
```

## Testing

Run tests with pytest:

```
pytest
```

## Project Structure

- `src/`: Source code for the ETL pipeline
- `tests/`: Unit tests
- `data/`: Sample data for testing