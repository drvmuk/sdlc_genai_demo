# Finance Transformation

This project implements a PySpark job to transform financial data from FAGLFLEXA and BSEG tables into a consolidated Finance table.

## Overview

The transformation process includes:
- Reading data from FAGLFLEXA and BSEG tables
- Filtering by fiscal year and posting period
- Joining with reference data (Golden Entity, GL, Trading Partner)
- Applying business logic for gain/loss calculations
- Writing transformed data to the Finance table

## Requirements

- Databricks Runtime 7.3 LTS
- Python 3.7+
- PySpark 3.1.2
- pandas 1.3.5
- numpy 1.21.6

## Setup

1. Upload the project files to your Databricks workspace
2. Install required libraries on your cluster
3. Configure mount points for source and target data locations

## Configuration

The job accepts the following parameters:

- `faglflexa_