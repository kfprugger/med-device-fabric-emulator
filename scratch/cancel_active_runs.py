import requests
from azure.identity import DefaultAzureCredential
import sys

def cancel_job(headers, workspace_id, item_id, job_type, run_id, name):
    # Try with job_type
    url1 = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/jobs/{job_type}/instances/{run_id}/cancel"
    # Try without job_type
    url2 = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{run_id}/cancel"
    # Try under datapipelines for pipelines
    url3 = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/datapipelines/pipelineruns/{run_id}/cancel"
    
    print(f"\nAttempting cancellation for {name} (Run ID: {run_id}) ...")
    
    for url, url_name in [(url1, "With Job Type"), (url2, "Without Job Type"), (url3, "DataPipelines Route")]:
        try:
            print(f"  Trying {url_name}: POST {url}")
            resp = requests.post(url, headers=headers)
            print(f"  Response: {resp.status_code} - {resp.text[:200]}")
            if resp.status_code in [200, 202]:
                print(f"✓ Successfully cancelled via {url_name}!")
                return True
        except Exception as e:
            print(f"  Error: {e}")
    return False

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    # 1. Cancel Spark notebook run
    notebook_id = "b95a98b9-3351-4f8b-ad1b-21ced09bbcfc" # fhir_ndjson_bronze_ingestion
    notebook_run_id = "63aeb334-ff35-4157-9aa4-07cee2ceb84f"
    
    # The jobType for the Spark run is "PipelineRunNotebook" or "SparkJob" - let's try "PipelineRunNotebook"
    cancel_job(headers, workspace_id, notebook_id, "RunNotebook", notebook_run_id, "Spark Notebook fhir_ndjson_bronze_ingestion")
    
    # 2. Cancel Ingestion pipeline run
    pipeline_id = "f8fc260d-6c1f-451b-8763-097fa274b4c0"
    pipeline_run_id = "62655b75-aa23-4b3b-8d48-ba5f6efa2a1d"
    cancel_job(headers, workspace_id, pipeline_id, "Pipeline", pipeline_run_id, "Clinical Ingestion Pipeline")

if __name__ == "__main__":
    main()
