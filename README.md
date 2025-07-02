# Order Summary Processor

A PySpark application that processes customer and order data into a Slowly Changing Dimension (SCD) Type 2 table.

## Overview

This application reads customer and order data from CSV files, cleans the data by removing null and duplicate records, and loads the data into a Slowly Changing Dimension (SCD) Type 2 table called "ordersummary".

## Requirements

- Databricks Runtime 10.4 LTS
- PySpark 3.3.0+
- Delta Lake 2.2.0+

## Setup

1. Upload the code to your Databricks workspace
2. Ensure the source data is available at the specified paths:
   - Customer data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - Order data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
3. Create a Databricks job with the following configuration:
   - Cluster: Databricks Cluster for Data Ingestion
   - Node Type: Standard_DS3_v2
   - Worker Nodes: 2-4 nodes (autoscaling enabled)
   - Auto Termination: 30 minutes
   - Libraries: PySpark, Delta Lake

## Usage

### Running the Application

To run the application as a Databricks job:

1. Create a new Databricks job
2. Set the main file to `src/order_summary_processor.py`
3. Configure the job to use the specified cluster
4. Run the job

### Running the Tests

To run the tests:

```bash
pytest -xvs tests/
```

## Sample Commands

### Running the Application Directly

```python
from src.order_summary_processor import OrderSummaryProcessor

# Initialize the processor
processor = OrderSummaryProcessor()

# Define paths
customer_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata"
order_path = "/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata"
target_table = "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary"

# Process the data
processor.process(customer_path, order_path, target_table)
```

### Querying the Result Table

```sql
SELECT * FROM gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary WHERE IsActive = true
```

## Data Flow

1. Read customer and order data from CSV files
2. Clean the data (remove nulls and duplicates)
3. Load cleaned data into delta tables
4. Join customer and order data on CustId
5. Update the SCD Type 2 table with new records

## Error Handling

The application includes error handling and logging, with a retry mechanism for transient errors. All errors and exceptions are logged to help with troubleshooting.