# Databricks SCD Pipeline

This project implements a Delta Live Tables pipeline for processing customer and order data with SCD Type 2 implementation in Databricks.

## Overview

The pipeline performs the following operations:
1. Reads customer and order data from volumes
2. Cleans and transforms the data (removing nulls, duplicates)
3. Calculates total amount for orders
4. Implements SCD Type 2 for tracking customer changes in the order summary
5. Creates an aggregate spend table by customer

## Setup

### Prerequisites
- Databricks workspace with access to Delta Live Tables
- Access to the volume paths specified in the configuration

### Installation
1. Upload this project to your Databricks workspace
2. Create a Delta Live Tables pipeline pointing to the main module

## Usage

### Running the Pipeline
Create a DLT pipeline in the Databricks UI:

1. Navigate to **Workflows** > **Delta Live Tables** > **Create Pipeline**
2. Configure the pipeline:
   - **Pipeline Name**: Customer Order SCD Pipeline
   - **Notebook Libraries**: Point to `src/dlt_pipeline.py`
   - **Target**: `gen_ai_poc_databrickscoe.sdlc_wizard`
   - **Storage Location**: Your preferred DBFS location
   - **Pipeline Mode**: Triggered or Continuous
3. Click **Create** and then **Start** to run the pipeline

### Sample Commands

To run the tests:
```bash
python -m pytest tests/
```

To manually execute the pipeline (non-DLT mode for testing):
```python
%run ./src/batch_processing.py
```

## Project Structure

- `src/`: Source code for the pipeline
  - `dlt_pipeline.py`: Main Delta Live Tables implementation
  - `batch_processing.py`: Batch processing version for testing
  - `config.py`: Configuration parameters
- `tests/`: Unit tests
- `data/`: Sample data for testing

## License
MIT