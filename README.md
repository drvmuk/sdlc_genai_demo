# Custom DQX Rules

This project implements custom DQX rules for data quality validation in Databricks environments.

## Overview

The project provides custom DQX rules for:
- Null value validation
- Primary key validation

These rules can be applied to any Spark DataFrame to validate data quality.

## Setup

1. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

2. Ensure the DQX library is installed on your Databricks cluster:
   ```
   %pip install dqx==0.7.0
   ```

## Usage

```python
from custom_dqx_rules.dqx_rules import DQXCustomRules

# Create a DQXCustomRules instance
dqx_rules = DQXCustomRules()

# Register custom rules
dqx_rules.register_rules()

# Apply DQ checks to a DataFrame
validation_results = dqx_rules.apply_dq_checks(input_df)

# Access validation results
validation_results.display()
```

## Sample Commands

To run tests:
```
pytest tests/
```

To execute in a Databricks notebook:
```python
%run ./src/custom_dqx_rules/dqx_rules
