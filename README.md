# DQX Custom Rules

This project implements custom data quality rules using the DQX library to validate input DataFrames in a Databricks environment.

## Overview

The implementation provides two custom data quality rules:
- `dqx_null_check`: Validates that specified columns do not contain null values
- `dqx_primary_check`: Validates that specified columns contain unique values (primary key check)

## Setup

1. Create a Databricks cluster with the following configuration:
   - Databricks Runtime Version: Latest compatible with DQX 0.7.0
   - Node Type: Standard_DS3_v2
   - Driver Node: 1 node with 14 GB memory, 4 cores
   - Worker Nodes: 2-5 nodes with 14 GB memory, 4 cores each
   - Autoscaling: Enabled (min 2, max 5 workers)
   - Auto Termination: 30 minutes

2. Install required libraries:
   ```
   databricks-labs-dqx==0.7.0
   PyYAML
   ```

## Usage

```python
from dqx_custom_rules.dq_engine import validate_dataframe

# Create or load your DataFrame
df = spark.read.format("delta").load("/path/to/data")

# Define your YAML metadata
yaml_metadata = """
checks:
  - columns: [id, name]
    rules: [dqx_null_check]
  - columns: [id]
    rules: [dqx_primary_check]
"""

# Validate the DataFrame
validated_df = validate_dataframe(df, yaml_metadata)

# Display validation results
validated_df.display()
```

## Testing

Run tests using pytest:

```
pytest
