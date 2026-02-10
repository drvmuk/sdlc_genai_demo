# E2E_AC_TXDBH_DP_YUYU_CREATION

This project implements the DP Address Change (YUYU) creation workflow using PySpark. It processes address change data from a staging Oracle table, performs necessary transformations, and produces an outbound flat file for downstream processing.

## Overview

The workflow:
1. Checks if there are source records to process
2. If records exist, generates the outbound DP Address Change (YUYU) file
3. Updates the staging table with processing status and audit fields
4. Handles trigger file creation and cleanup operations

## Setup

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Configure environment variables in your Databricks cluster:
```
DBConnection_E2E_ORA_STG=jdbc:oracle:thin:@//your_stg_host:port/service_name
DBConnection_E2E_ORA_ODS=jdbc:oracle:thin:@//your_ods_host:port/service_name
```

3. Set up secrets for database credentials in Databricks secrets:
```
databricks secrets create-scope --scope e2e_oracle
databricks secrets put --scope e2e_oracle --key stg_username
databricks secrets put --scope e2e_oracle --key stg_password
databricks secrets put --scope e2e_oracle --key ods_username
databricks secrets put --scope e2e_oracle --key ods_password
```

## Usage

Run the main workflow:
```
python src/main.py \
  --stg_schema ZSYSE2EDEV \
  --stg_table STG_E2E_AC_TXDBH_DATA \
  --lkp_schema ODS_SCHEMA \
  --lkp_tbl_1 T_YUYU_CLNT \
  --lkp_tbl_2 T_YUYUK_CLN \
  --output_dir /dbfs/mnt/target_files \
  --output_file_yuyu DPAddressChangeYUYU_$(date +%Y%m%d%H%M%S).txt \
  --trigger_file DPACYUYU_TriggerFile.txt \
  --process_userid ETL_USER
```

## Testing

Run tests:
```
pytest tests/
