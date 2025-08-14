# Finance Data Pipeline

## Overview
This pipeline processes financial data from FAGLFLEXA and BSEG source tables in the Everest ECC schema and populates the Finance table with transformed data.

## Technical Details
- **Technical Requirement ID:** TR-FIN-001
- **Related Functional Requirement(s):** FR-FIN-001

## Pipeline Components

### 1. Notebook
- **Path:** `/notebooks/finance_data_pipeline`
- **Purpose:** Main processing logic for the Finance data pipeline
- **Functions:**
  - Load source tables (FAGLFLEXA, BSEG, Golden Views)
  - Transform financial data according to business requirements
  - Write transformed data to target Finance table

### 2. Job Configuration
- **Path:** `conf/finance_pipeline_job.json`
- **Purpose:** Defines the scheduled job that runs the Finance data pipeline
- **Schedule:** Daily at 3:00 AM UTC

### 3. Cluster Configuration
- **Path:** `cluster/finance_cluster_config.json`
- **Purpose:** Defines the Databricks cluster configuration for the Finance data pipeline
- **Specs:** 
  - Databricks Runtime: 7.3 LTS
  - Node Type: Standard_DS3_v2
  - Autoscaling: 2-5 worker nodes

### 4. Initialization Script
- **Path:** `init_scripts/finance_cluster_init.sh`
- **Purpose:** Sets up the cluster environment for the Finance data pipeline

## Deployment Instructions

1. Create the Databricks cluster using the configuration in `cluster/finance_cluster_config.json`
2. Upload the initialization script to `dbfs:/databricks/init/finance_cluster_init.sh`
3. Import the notebook to `/notebooks/finance_data_pipeline`
4. Create the job using the configuration in `conf/finance_pipeline_job.json`
5. Set up appropriate permissions for the job and notebook

## Monitoring and Maintenance

- Pipeline logs are stored in `/dbfs/logs/finance_pipeline/`
- Checkpoints for incremental processing are stored in `/dbfs/checkpoints/finance_pipeline/`
- Email notifications are configured to alert the Finance Data Team on job failures

## Dependencies

- Access to Everest ECC schema (FAGLFLEXA and BSEG tables)
- Access to Golden Views schema (entity, gl_account, trading_partner views)
- Write permissions to Finance schema