# Product Data ETL Pipeline

This project implements an end-to-end ETL pipeline for product data using Delta Live Tables (DLT) on Databricks. It ingests raw data from CSV files, cleans and transforms the data, and aggregates it into a final reporting table.

## Project Structure

```
project_name/
│
├── src/
│   ├── __init__.py
│   ├── bronze_dlt.py  # DLT pipeline for the Bronze layer
│   ├── silver_dlt.py  # DLT pipeline for the Silver layer
│   └── gold_dlt.py    # DLT pipeline for the Gold layer
│
├── tests/
│   ├── test_bronze_dlt.py # Tests for Bronze layer
│   ├── test_silver_dlt.py # Tests for Silver layer
│   └── test_gold_dlt.py   # Tests for Gold Layer
│
├── data/              # Sample input data (CSV files)
├── requirements.txt # List of required Python packages
├── pyproject.toml   # Project metadata and dependencies
├── README.md        # This file
├── LICENSE        # Project license
└── .gitignore     # Specifies intentionally untracked files that Git should ignore
```

## Setup

1.  **Install Dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure Databricks CLI:**

    Make sure you have the Databricks CLI configured with a valid access token.

3.  **Create Unity Catalog Volumes:**

    Create Unity Catalog Volumes as specified in the TRD for the raw data.

4.  **Upload Sample Data:**

    Upload the sample CSV data files to the configured Unity Catalog Volumes paths.

## Usage

1.  **Create a Databricks Cluster:**

    Create a Databricks cluster with the configuration specified in the TRD (Runtime 14.3.x-photon-scala2.12, autoscaling enabled).

2.  **Create a Delta Live Tables Pipeline:**

    Create a new DLT pipeline in Databricks.

3.  **Configure the Pipeline:**

    *   Source Code: Upload the `src/` directory containing the DLT pipeline code (`bronze_dlt.py`, `silver_dlt.py`, `gold_dlt.py`).  Specify `src/bronze_dlt.py` as the primary notebook/file. DLT will auto-discover dependent files.
    *   Target:  Set the target catalog and schema for the pipeline (e.g., `dev_catalog.product_data`).
    *   Configuration: Add any necessary configuration parameters (e.g., grouping column for the Gold layer).
    *   Cluster:  Select the cluster created in step 1.

4.  **Run the Pipeline:**

    Start the DLT pipeline.  The pipeline will automatically ingest, transform, and load the data.

## Sample Commands (Databricks CLI)

*Not yet available, requires implementing the Databricks CLI tool to run the DLT pipelines*

## Testing

Run the tests using `pytest` to verify the pipeline's functionality:

```bash
pytest tests/
```

## Notes

*   This pipeline assumes that the input CSV files are properly formatted and adhere to the expected schema.
*   Data quality checks using DLT expectations are implemented to ensure data accuracy.
*   SCD Type 2 logic is implemented to track historical changes in the data.
