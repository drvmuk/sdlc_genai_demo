# Customer Order Data Processing

A data processing system for loading customer and order data into Delta tables and generating order summaries using PySpark and Delta Lake.

## Overview

This project implements a scalable data processing system that:

1. Loads customer and order data from CSV files into Delta tables
2. Removes null and duplicate records
3. Generates order summaries by joining customer and order data
4. Updates order summaries when customer data changes
5. Implements SCD Type 2 logic for tracking historical changes

## Setup

### Prerequisites

- Databricks Runtime 10.4 LTS or higher
- PySpark 3.3.0 or higher
- Delta Lake 2.2.0 or higher

### Installation

1. Clone this repository to your Databricks workspace
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Loading Data and Generating Order Summary

Run the following command to load customer and order data and generate the initial order summary:

```bash
python -m src.main --job load_data
```

### Updating Order Summary for Customer Changes

Run the following command to update the order summary when customer data changes:

```bash
python -m src.main --job update_summary
```

## Testing

Run the tests using pytest:

```bash
pytest
```

## Project Structure

- `src/`: Source code for the data processing system
  - `data_loader.py`: Contains the DataLoader class for loading and processing data
  - `main.py`: Main entry point for running the data processing jobs
- `tests/`: Unit tests for the data processing system
- `data/`: Sample data files for testing

## Configuration

The system is configured to work with the following data sources and targets:

- Customer data source: `Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
- Order data source: `Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
- Customer Delta table: `gen_ai_poc_databrickscoe.sdlc_wizard.customer`
- Order Delta table: `gen_ai_poc_databrickscoe.sdlc_wizard.order`
- Order summary Delta table: `gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary`