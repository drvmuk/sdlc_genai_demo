# Delta Live Tables (DLT) Implementation

This project implements a Delta Live Tables (DLT) pipeline to process customer and order data, create joined tables, implement SCD Type 2 logic, and generate aggregated customer spend data.

## Overview

The pipeline consists of the following stages:
1. Load customer and order data from CSV files into Delta tables
2. Create an ordersummary table by joining customer and order data
3. Implement SCD Type 2 logic for the ordersummary table
4. Create a customeraggregatespend table with aggregated data

## Setup

1. Clone this repository to your Databricks workspace
2. Install required packages: `pip install -r requirements.txt`
3. Configure the DLT pipeline using the provided notebook

## Usage

To run the DLT pipeline:

1. Upload the notebook to your Databricks workspace
2. Create a new DLT pipeline pointing to the notebook
3. Configure the pipeline with the appropriate cluster settings
4. Run the pipeline

## Configuration

The pipeline is configured to run on the following cluster:
- Databricks Runtime Version: 10.4.x-scala2.12
- Node Type: Standard_DS3_v2
- Worker Nodes: 2-4 Standard_DS3_v2 (autoscaling enabled)
- Auto Termination: 30 minutes

## Directory Structure

```
dlt_implementation/
│
├── src/
│   ├── __init__.py
│   ├── dlt_pipeline.py
│   └── utils.py
│
├── tests/
│   ├── test_dlt_pipeline.py
│   └── test_utils.py
│
├── data/                
│   ├── customer_sample.csv
│   └── order_sample.csv
│
├── requirements.txt     
├── pyproject.toml       
├── README.md            
├── LICENSE
└── .gitignore
