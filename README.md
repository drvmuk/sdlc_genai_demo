# Finance Data Processor

This project implements a PySpark job to extract data from Everest ECC source tables, apply transformations, and load the data into the Finance table.

## Overview

The job processes data from the following sources:
- Everest ECC FAGLFLEXA table
- Everest ECC BSEG table
- Golden Entity view
- Golden GL view
- Golden Trading Partner view
- BPC exchange rates

The data is transformed according to business rules and loaded into the Finance table.

## Requirements

- Databricks Runtime 7.3 LTS or higher
- PySpark 3.1.1 or higher
- Python 3.7 or higher
- Access to ADLS storage and Hive Metastore

## Setup

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure access to ADLS storage and Hive Metastore

## Usage

### Running the Job

```bash
python -m src.finance_processor
```

### Configuration

The job configuration is defined in `src/config.py`. Update the paths and parameters as needed.

## Testing

Run the tests using pytest:

```bash
pytest tests/
```

## Deployment

Deploy the job to a Databricks cluster with the following configuration:
- Cluster Name: Finance Processing Cluster
- Databricks Runtime Version: 7.3 LTS
- Node Type: Standard_DS3_v2
- Driver Node: 1 x Standard_DS3_v2
- Worker Nodes: 2-5 x Standard_DS3_v2 (autoscaling)
- Autoscaling: Enabled
- Auto Termination: Enabled (30 minutes)