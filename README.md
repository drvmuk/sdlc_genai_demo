# Finance Data Pipeline

This project implements a data pipeline to load finance data from ECC Everest into the target table "Finance" based on the specified transformation logic.

## Overview

The Finance Data Pipeline extracts data from SAP ECC tables (FAGLFLEXA and BSEG), applies transformations according to business requirements, and loads the result into a target Finance table. The pipeline includes error handling, logging, and email notifications for critical errors.

## Architecture

The pipeline follows an ETL (Extract, Transform, Load) pattern:

1. **Extract**: Data is retrieved from FAGLFLEXA and BSEG tables in SAP ECC Everest, as well as from golden views for entity, GL account, and trading partner data.
2. **Transform**: The data is joined, filtered, and transformed according to business rules.
3. **Load**: The transformed data is loaded into the target Finance table.

## Requirements

- Databricks Runtime 7.3 LTS
- PySpark 3.1.2
- Python 3.7+
- Access to SAP ECC Everest database
- Access to golden views database
- Write access to target database

## Setup

1. Clone this repository to your Databricks workspace.
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Configure the database connection parameters in the code.
4. Configure email notification settings.

## Usage

### Running the Pipeline

To run the pipeline in Databricks:

```python
from src.finance_data_pipeline import main

result = main()
print(f"Pipeline status: {result['status']}")
```

### Scheduling

Schedule the pipeline using Databricks Jobs:

1. Create a new job
2. Set the cluster configuration as specified in the Technical Requirements
3. Add a notebook task that calls the main function
4. Set the schedule according to your requirements (e.g., daily at 2 AM)

## Testing

Run the tests using pytest:

```bash
pytest tests/
```

## Monitoring

The pipeline logs all operations and errors. You can monitor the pipeline execution through:

1. Databricks logs
2. Email notifications for critical errors
3. Job run history in Databricks

## Cluster Configuration

- Cluster Name: Finance Data Processing Cluster
- Databricks Runtime Version: 7.3 LTS
- Node Type: Standard_DS3_v2
- Driver Node: 1 x Standard_DS3_v2
- Worker Nodes: 2-5 x Standard_DS3_v2 (autoscaling)
- Autoscaling: Enabled
- Auto Termination: Enabled (30 minutes)
- Libraries Installed: PySpark, pandas, numpy