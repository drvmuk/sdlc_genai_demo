# Databricks notebook with %run magic command to run the pipeline

# COMMAND ----------
# This file contains the code to run the DLT pipeline from a notebook

def create_and_run_pipeline():
    """
    Creates and runs the DLT pipeline using the Databricks Jobs API
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs
    
    # Initialize the Workspace client
    ws = WorkspaceClient()
    
    # Define the pipeline configuration
    pipeline_name = "Customer Order Data Pipeline"
    
    pipeline_config = {
        "name": pipeline_name,
        "clusters": [
            {
                "label": "default",
                "num_workers": 2,
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 4
                }
            }
        ],
        "development": True,
        "continuous": False,
        "libraries": [
            {
                "notebook": {
                    "path": "/Repos/your_repo_path/src/dlt_pipeline"
                }
            }
        ],
        "target": f"{CATALOG}.{SCHEMA}",
        "configuration": {
            "pipelines.enableTrackHistory": "true"
        }
    }
    
    # Create or update the pipeline
    try:
        existing_pipelines = ws.pipelines.list(filter=f"name='{pipeline_name}'")
        pipeline_id = None
        
        for pipeline in existing_pipelines:
            if pipeline.name == pipeline_name:
                pipeline_id = pipeline.pipeline_id
                break
        
        if pipeline_id:
            # Update existing pipeline
            ws.pipelines.edit(pipeline_id=pipeline_id, **pipeline_config)
            print(f"Updated existing pipeline with ID: {pipeline_id}")
        else:
            # Create new pipeline
            response = ws.pipelines.create(**pipeline_config)
            pipeline_id = response.pipeline_id
            print(f"Created new pipeline with ID: {pipeline_id}")
        
        # Start a pipeline update
        ws.pipelines.start_update(pipeline_id=pipeline_id)
        print(f"Started pipeline update for pipeline ID: {pipeline_id}")
        
    except Exception as e:
        print(f"Error creating or running pipeline: {str(e)}")

# COMMAND ----------
# Run the function to create and start the pipeline
create_and_run_pipeline()