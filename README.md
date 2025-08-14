# Finance Data Transformation

This project implements a Spark SQL job to generate finance data by transforming and processing data from Everest ECC source tables FAGLFLEXA and BSEG.

## Overview

The finance data transformation process retrieves data from FAGLFLEXA and BSEG tables, applies transformation logic, calculates GainLossGC, determines OffsetAccount, and stores the transformed data in the target Finance table.

## Setup

1. Upload the project to your Databricks workspace
2. Install the required dependencies
3. Configure the cluster as specified in the requirements

## Usage

To run the finance data transformation job:

```bash
# Run the main transformation job
python -m src.finance_transformation

# Run tests
pytest
```

## Cluster Configuration

- **Cluster Name**: Finance Processing Cluster
- **Databricks Runtime Version**: 7.3 LTS
- **Node Type**: Standard_DS3_v2
- **Driver Node**: 1 x Standard_DS3_v2
- **Worker Nodes**: 2-5 x Standard_DS3_v2 (autoscaling)
- **Autoscaling**: Enabled
- **Auto Termination**: Enabled (30 minutes)
- **Libraries Installed**: Spark SQL, Scala 2.12