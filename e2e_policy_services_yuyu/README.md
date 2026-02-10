# E2E Policy Services - DP Address Change YUYU Extract

## Overview

This PySpark application implements the Informatica mapping `m_E2E_AC_TXDBH_DP_YUYU_CREATION` for producing a downstream "DP Address Change – YUYU" extract and updating staging control/audit attributes.

## Features

- Reads address change candidate records from Oracle staging table `STG_E2E_AC_TXDBH_DATA`
- Enriches data with policy owner names via lookups (`T_YUYU_CLNT`, `T_YUYUK_CLN`)
- Applies business rules for address fallback, postal code normalization, and phone formatting
- Generates flat-file style output `DPAddressChangeYUYU` with sequence numbers
- Updates staging table with processing metadata (process date, process user)

## Architecture

```
Source (Oracle) → Enrichment (Lookups) → Transformations → Dual Output:
                                                            1. DPAddressChangeYUYU (Parquet/CSV)
                                                            2. STG_E2E_AC_TXDBH_DATA (Update)
```

## Business Rules Implemented

- **BR-01**: Stamp processing date (SYSDATE)
- **BR-02**: Stamp processing user (from parameter)
- **BR-03**: Reception date formatting
- **BR-04**: Address Kana fallback to PPAY_* when ZIP missing
- **BR-05**: Postal code normalization (remove hyphens)
- **BR-06**: Telephone number formatting (Japanese format)
- **BR-07**: Policy owner name derivation (Kana)
- **BR-08**: Policy owner name derivation (Kanji)

## Setup

### Prerequisites

- Databricks Runtime 11.3+ or Apache Spark 3.3+
- Python 3.8+
- JDBC driver for Oracle (ojdbc8.jar)

### Installation

```bash
pip install -r requirements.txt
```

### Configuration

Set environment variables or update `config.py`:

```bash
export ORACLE_HOST=your_oracle_host
export ORACLE_PORT=1521
export ORACLE_SERVICE=your_service
export ORACLE_USER=ZSYSE2EDEV
export ORACLE_PASSWORD=your_password
export M_PROCESS_USERID=ETL_USER
export OUTPUT_PATH=/mnt/output/yuyu
```

## Usage

### Run the full pipeline

```bash
spark-submit \
  --jars /path/to/ojdbc8.jar \
  --master local[*] \
  src/main.py
```

### Run tests

```bash
pytest tests/ -v
```

## Project Structure

```
e2e_policy_services_yuyu/
├── src/
│   ├── __init__.py
│   ├── main.py                 # Entry point
│   ├── config.py               # Configuration and parameters
│   ├── data_loader.py          # Source and lookup data loading
│   ├── transformations.py      # Business rule transformations
│   └── data_writer.py          # Output writers
├── tests/
│   ├── test_transformations.py # Unit tests for business rules
│   └── test_integration.py     # Integration tests
├── data/                       # Sample data for testing
├── requirements.txt
├── pyproject.toml
├── README.md
├── LICENSE
└── .gitignore
```

## Sample Output

### DPAddressChangeYUYU Schema

| Column | Type | Description |
|--------|------|-------------|
| SEQUENCE_NUMBER | int | Auto-generated sequence |
| ORIGIN_REQUEST_ID | string | Origin request identifier |
| POLICY_ID | string | Policy number |
| RECEPTION_DATE | timestamp | Request acceptance datetime |
| NEW_ADDRESS_POSTAL_CODE | string | Normalized postal code |
| NEW_ADDRESS_KANA_1/2/3 | string | Address lines (Kana) |
| NEW_ADDRESS_KANJI_1/2/3 | string | Address lines (Kanji) |
| NEW_ADDRESS_TELEPHONE_NUMBER | string | Formatted phone number |
| POLICY_OWNER_NAME_KANA | string | Owner name (Kana) |
| POLICY_OWNER_NAME_KANJI | string | Owner name (Kanji) |

## Known Issues / Open Items

1. **Reception Date Format**: TO_CHAR and TO_DATE format masks differ in source spec (YYYY/MM/DD vs YYYY-MM-DD)
2. **Phone Formatting**: Last segment uses 6 digits; confirm with business
3. **Source Filtering**: No filter criteria provided; processes all records
4. **Lookup Duplicates**: "Use Any Value" policy; non-deterministic if duplicates exist

## License

Proprietary - Internal Use Only