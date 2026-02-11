# BNCPLS IF23B 10K File Generation

This project implements the PySpark-based data pipeline for generating the "10K" flat files from Oracle source tables as specified in the BNCPLS IF23B 10K File Generation functional requirements document.

## Overview

The pipeline extracts data from Oracle source tables (T_BENEFICIARY and T_APPLICATION_STAGING), processes AURA payloads, calculates required fields, and generates flat files with a maximum of 10,000 records per file.

## Features

- Oracle data extraction with parameterized SQL
- AURA payload parsing and XML transformation
- Field calculation and standardization
- File splitting to maintain 10K records per file
- Comprehensive error handling and logging

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables:
```bash
export SPARK_HOME=/path/to/spark
export PYTHONPATH=$PYTHONPATH:/path/to/project
```

## Usage

Run the main pipeline:

```bash
spark-submit --master databricks \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  --conf spark.databricks.io.cache.enabled=true \
  src/main.py \
  --src_sql "SELECT * FROM T_APPLICATION_STAGING a JOIN T_BENEFICIARY b ON a.POLICY_NUMBER = b.POLICY_NUMBER WHERE a.PROCESS_DATE = '2023-05-01'" \
  --xslt_file "/path/to/aura14_transform.xslt" \
  --aura15_xslt_file "/path/to/aura15_transform.xslt" \
  --output_dir "/output/path" \
  --file_prefix "IF23B" \
  --file_ext "dat" \
  --batch_id "20230501" \
  --max_rows_per_file 10000
```

## Testing

Run the test suite:

```bash
pytest
```

## License

Copyright (c) 2023 Data Engineering Team. All rights reserved.