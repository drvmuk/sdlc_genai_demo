# Databricks Delta Lake ETL Pipeline

This project implements a comprehensive ETL pipeline using Databricks, PySpark, and Delta Lake. The pipeline processes customer and order data, performing various transformations, cleaning operations, and implementing SCD Type 2 for historical tracking.

## Features

- Load customer and order data from CSV to Delta tables
- Transform order data by adding calculated columns
- Clean data by removing nulls and duplicates
- Create joined order summary table
- Implement SCD Type 2 for history tracking
- Create customer aggregate spend analysis
- Full Delta Live Tables (DLT) implementation

## Setup

1. Configure a Databricks cluster with the following specifications:
   - Databricks Runtime Version: 10.4.x-scala2.12
   - Node Type: Standard_DS3_v2
   - Driver Node: Standard_DS3_v2
   - Worker Nodes: 2-4 Standard_DS3_v2
   - Autoscaling: Enabled
   - Auto Termination: 30 minutes

2. Install required libraries:
   - delta-lake
   - spark-csv

3. Mount or configure access to the source data locations:
   - Customer data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
   - Order data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

## Usage

### Standard PySpark Jobs

Run the individual PySpark modules in sequence:

1. `python src/load_data.py` - Load CSV data to Delta tables
2. `python src/transform_data.py` - Add TotalAmount column
3. `python src/clean_data.py` - Remove nulls and duplicates
4. `python src/create_order_summary.py` - Join customer and order data
5. `python src/implement_scd_type2.py` - Implement SCD Type 2
6. `python src/create_customer_aggregate.py` - Create aggregate spend table

### Delta Live Tables

Alternatively, use the DLT implementation which combines all steps into a single pipeline:

```
python src/dlt_pipeline.py
```

## Testing

Run the test suite to verify functionality:

```
pytest tests/
```

## License

MIT