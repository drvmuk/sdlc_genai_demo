# Customer Order Pipeline

A Delta Live Tables pipeline for processing customer and order data, implementing SCD Type 2 pattern and aggregation.

## Overview

This project implements a data pipeline using Databricks Delta Live Tables (DLT) that:

1. Ingests customer and order data from CSV files
2. Cleans and transforms the data (removing nulls, duplicates)
3. Adds calculated fields (TotalAmount)
4. Implements SCD Type 2 pattern for tracking changes in customer data
5. Creates aggregated customer spend reports

## Architecture

The pipeline consists of these main tables:

- `customer`: Raw customer data with cleaning applied
- `order`: Order data with cleaning applied and TotalAmount calculation
- `ordersummary`: SCD Type 2 table combining customer and order data
- `customeraggregatespend`: Aggregated customer spending by name and date

## Setup and Installation

1. Clone this repository to your Databricks workspace
2. Ensure you have the required volumes set up:
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
3. Install required packages listed in requirements.txt

## Usage

### Running the Pipeline

You can run the pipeline in two ways:

1. **Using the notebook_runner.py script**:
   - Open the notebook in Databricks
   - Run all cells to create and start the pipeline

2. **Manually creating a DLT pipeline**:
   - In the Databricks workspace, go to Workflows > Delta Live Tables
   - Create a new pipeline
   - Add the `src/dlt_pipeline.py` file as the source
   - Configure the target as `gen_ai_poc_databrickscoe.sdlc_wizard`
   - Start the pipeline

### Sample Commands

To manually run the pipeline from the Databricks CLI:

```bash
databricks pipelines create --settings pipeline-config.json
databricks pipelines start --pipeline-id <pipeline-id>
```

To query the resulting tables:

```sql
SELECT * FROM gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary WHERE IsActive = true;

SELECT Name, Date, TotalAmount 
FROM gen_ai_poc_databrickscoe.sdlc_wizard.customeraggregatespend
ORDER BY TotalAmount DESC;
```

## Testing

Run the tests using pytest:

```bash
pytest tests/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.