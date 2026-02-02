# SQL Generation for Sales Orders Data Pipeline

This project provides PySpark code to generate SQL statements for creating sales order tables based on mapping documents. It implements a two-step process:

1. Create an intermediate table `sales_orders_comp_stg` from source tables
2. Create the main table `sales_orders_comp` using the intermediate table and additional sources

## Setup

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run the code in a Databricks environment

## Usage

```python
from src.sql_generator import generate_sales_orders_stg_sql, generate_sales_orders_sql

# Generate SQL for intermediate staging table
stg_sql = generate_sales_orders_stg_sql()
print(stg_sql)

# Generate SQL for main sales orders table
main_sql = generate_sales_orders_sql()
print(main_sql)

# Execute SQL in Databricks
spark.sql(stg_sql)
spark.sql(main_sql)
```

## Project Structure

- `src/sql_generator.py`: Core SQL generation logic
- `tests/`: Unit tests for SQL generation
- `data/`: Sample data for testing

## Requirements

- PySpark 3.0+
- Databricks Runtime 7.0+