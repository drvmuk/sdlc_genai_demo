# Transaction Analytics

A PySpark application for processing and analyzing transaction data at scale. This project demonstrates best practices for building data processing pipelines using Databricks and PySpark.

## Features

- Data ingestion from various sources
- Data cleaning and transformation
- Customer transaction analysis
- Aggregation and reporting
- Delta Lake integration for ACID transactions

## Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Configure data sources in the config module

## Usage

### Running in Databricks

1. Upload the project to your Databricks workspace
2. Create a job that runs the main module:

```python
from transaction_analytics.src.main import run_pipeline

run_pipeline(date="2023-10-01")
```

### Running locally (for development)

```bash
python -m transaction_analytics.src.main --date 2023-10-01
```

## Testing

Run tests using pytest:

```bash
pytest
```

## Project Structure

- `src/`: Source code
  - `data_loader.py`: Functions for loading data from various sources
  - `transformer.py`: Data transformation logic
  - `main.py`: Main pipeline orchestration
- `tests/`: Unit tests
- `data/`: Sample data for testing

## Contributing

Please refer to our contributing guidelines for details on how to contribute to this project.