# Customer Order ETL Pipeline

This project implements an ETL pipeline for processing customer and order data using PySpark and Databricks Delta Lake. The pipeline ingests data from CSV files, cleanses it, and loads it into a Delta Lake table with SCD Type 2 logic for tracking historical changes.

## Features

- Data ingestion from CSV files
- Data cleansing (removal of nulls and duplicates)
- Delta Lake integration
- SCD Type 2 implementation for tracking historical changes
- Error handling and logging

## Requirements

- Databricks Runtime 13.3 LTS (includes Apache Spark 3.4.1, Scala 2.12)
- Python 3.8+
- PySpark 3.4.1
- Delta Lake 2.4.0

## Project Structure

```
customer_order_etl/
│
├── src/                    # Source code
│   ├── __init__.py
│   ├── etl_processor.py    # ETL processing logic
│   └── main.py             # Main entry point
│
├── tests/                  # Unit tests
│   ├── test_etl_processor.py
│   └── test_main.py
│
├── data/                   # Sample data
│   ├── sample_customer_data.csv
│   └── sample_order_data.csv
│
├── requirements.txt        # Python dependencies
├── pyproject.toml          # Project metadata
├── README.md               # Project documentation
└── LICENSE                 # License information
```

## Installation

Clone the repository and install the required packages:

```bash
pip install -e .
```

## Usage

### Running in Databricks

1. Upload the project files to Databricks DBFS
2. Create a new Databricks notebook
3. Import the main module and run the ETL pipeline:

```python
# Import the main module
from src.main import main

# Run the ETL pipeline
main()
```

### Running Tests

To run the tests:

```bash
pytest tests/
```

## ETL Pipeline Flow

1. **Data Ingestion**: Read customer and order data from CSV files
2. **Data Cleansing**: Remove null values and duplicate records
3. **Table Creation**: Create the ordersummary table if it doesn't exist
4. **Initial Load**: Join customer and order data and load into the ordersummary table
5. **SCD Type 2 Updates**: Apply SCD Type 2 logic for subsequent runs to track historical changes

## Sample Commands

To run the ETL pipeline manually:

```python
from pyspark.sql import SparkSession
from src.etl_processor import ETLProcessor

# Create SparkSession
spark = SparkSession.builder \
    .appName("CustomerOrderETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Initialize and run ETL processor
etl_processor = ETLProcessor(spark)
etl_processor.run_etl_pipeline()
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.