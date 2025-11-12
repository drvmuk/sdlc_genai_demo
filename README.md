# Customer Order Processing

This project processes customer and order data, implementing both batch processing and Delta Live Tables approaches. It includes SCD Type 2 implementation for tracking historical changes in customer data.

## Features

- Data loading from CSV files
- Data cleaning (removing nulls and duplicates)
- SCD Type 2 implementation for tracking customer changes
- Aggregation of customer spending
- Implementation using both batch processing and Delta Live Tables

## Project Structure

- `src/batch_processing.py`: Contains the batch processing implementation
- `src/delta_live_tables.py`: Contains the Delta Live Tables implementation
- `tests/`: Contains unit tests for both implementations
- `data/`: Contains sample data files for testing

## Setup

1. Install the required packages:

```bash
pip install -r requirements.txt
```

2. Make sure you have access to the data volumes:
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

## Usage

### Batch Processing

To run the batch processing job:

```python
from src.batch_processing import main

main()
```

### Delta Live Tables

To deploy the Delta Live Tables pipeline, create a DLT pipeline in the Databricks UI with the following settings:

- Source file: `src/delta_live_tables.py`
- Target: `gen_ai_poc_databrickscoe.sdlc_wizard`
- Configuration:
  - `spark.databricks.delta.properties.defaults.enableChangeDataFeed`: `true`

## Testing

Run the tests using pytest:

```bash
pytest
```

## Data Flow

1. Read customer and order data from source volumes
2. Clean data by removing nulls and duplicates
3. Add TotalAmount column to order data
4. Create ordersummary table with SCD Type 2 implementation
5. Create customeraggregatespend table with aggregated spending by customer and date

## SCD Type 2 Implementation

The SCD Type 2 implementation tracks changes in customer data by:

1. Marking old records as inactive when customer details change
2. Adding new records with updated customer details
3. Maintaining start and end dates for each record
4. Preserving the history of all changes