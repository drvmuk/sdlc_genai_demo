# E2E Policy Services Data Extraction

This project implements a PySpark ETL pipeline to extract policy data from various source tables in the E2E_POLICYSERVICES folder and load it into the STG_E2E_BC_K2H_TXDB_DATA target table.

## Overview

The pipeline extracts data from the following source tables:
- T_TX_REQUEST_POLICY
- T_TX_BASIC
- T_TX_RELATION
- T_TX_REQUEST
- T_TX_DTL_BENEFICIARY_CHANGE

It performs necessary transformations and joins to create a consolidated view of policy data.

## Setup

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Configure database connections in the config file.

3. Run the pipeline:
```
python -m src.main
```

## Project Structure

- `src/`: Contains the main ETL code
- `tests/`: Contains unit tests
- `data/`: Sample data for testing

## Requirements

- Python 3.8+
- PySpark 3.1+
- Databricks Runtime 9.1+

## License

MIT