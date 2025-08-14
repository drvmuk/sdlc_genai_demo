# Finance Data Transformation

This project transforms finance data from FAGLFLEXA and BSEG tables into a target Finance table according to TR-FIN-001 requirements.

## Overview

The transformation process includes:
- Filtering FAGLFLEXA records based on RLDNR = '0L'
- Joining FAGLFLEXA with BSEG on specific conditions
- Applying business transformations including currency conversions, GL account mappings, and entity mappings
- Loading the transformed data into the target Finance table

## Setup

1. Create a Databricks cluster with the following configuration:
   - Databricks Runtime Version: 7.3 LTS or later
   - Node Type: Standard_DS3_v2
   - Worker Nodes: 2-5 (autoscaling)
   - Auto Termination: 30 minutes

2. Install required libraries:
   ```
   pip install -r requirements.txt
   ```

## Usage

Run the main transformation script:

```python
%run ./src/finance_transformation.py
```

## Parameters

The transformation requires two parameters:
- `fiscal_year`: The fiscal year for data processing
- `posting_period`: The posting period for data processing

Example:
```python
fiscal_year = "2023"
posting_period = "12"
