# Finance Data Processor

This project implements a data processing job to transform and load finance data from the ECC Everest source system into the target Finance table.

## Overview

The pipeline retrieves data from FAGLFLEXA and BSEG tables, joins them, applies transformation logic, and loads the transformed data into the target Finance table.

## Setup

1. Clone this repository to your Databricks workspace
2. Install required dependencies: `pip install -r requirements.txt`
3. Configure the cluster according to specifications in the Technical Requirements Document

## Usage

Run the main processing job with parameters:

```python
dbutils.notebook.run("src/finance_processor.py", 
                     timeout_seconds=3600, 
                     arguments={"fiscal_year": "2023", "posting_period": "12"})
```

## Configuration

The job is configured to run on a Finance Data Processing Cluster with the following specifications:
- Databricks Runtime Version: 7.3 LTS
- Node Type: Standard_DS3_v2
- Worker Nodes: 2-5 (autoscaling)
- Auto Termination: 30 minutes

## Testing

Run tests using pytest:
```
pytest tests/
