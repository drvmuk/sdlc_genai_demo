Project: dlt_orders_pipeline

Contents
- src/python/pipelines/dlt_orders_pipeline.py: PySpark Delta Live Tables (DLT) pipeline implementing the TRD
- configs/pipeline_settings.json: Template pipeline configuration for Databricks DLT
- src/python/main.py: Minimal entrypoint
- tests/test_main.py: Minimal unit test

Prerequisites
- Databricks workspace with Unity Catalog enabled
- Databricks Runtime supporting DLT (e.g., DBR 13.3 LTS or higher)
- Source CSVs placed in Unity Catalog Volumes:
  - /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata
  - /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata

Local development
- Python 3.10+
- Optional: pip install pytest

Run tests locally
- From repository root:
  - pip install -r requirements.txt (if you add dependencies)
  - pytest -q

Deploy to Databricks as a DLT Pipeline
1) Import repo
- Push this repository to Databricks Repos or workspace files. Ensure the path to dlt_orders_pipeline/src/python/pipelines/dlt_orders_pipeline.py is accessible.

2) Configure pipeline
- Open Workflows > Delta Live Tables > Create Pipeline
- Name: dlt_orders_pipeline (or any)
- Storage location: dbfs:/pipelines/<your-pipeline-storage-path>
- Target: gen_ai_poc_databrickscoe.sdlc_wizard
- Source: Add a Notebook/Script library pointing to src/python/pipelines/dlt_orders_pipeline.py
- Cluster mode: Select an appropriate DBR version (e.g., 13.3.x) and worker type/size
- Advanced: Enable Photon optionally

3) Permissions
- Ensure the pipeline has permissions to read from the source Unity Catalog Volumes and write to the target catalog.schema.

4) Start the pipeline
- Click Start to run a full refresh. Subsequent runs can be triggered on schedules.

Notes
- Expectations are defined to drop invalid records for required fields and non-negative amounts. If you prefer quarantine, change expect_or_drop to expect and route failures to a separate table.
- ordersummary implements SCD Type 2 using dlt.apply_changes with CustId+OrderId as keys.
- customeraggregatespend aggregates active SCD2 rows by Name and Date.

Troubleshooting
- If the pipeline cannot find the script path, adjust the libraries path in configs/pipeline_settings.json to your workspace location.
- Validate source files conform to the enforced schemas.
