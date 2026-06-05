import requests
from azure.identity import DefaultAzureCredential
import sys
import json

def check_details(workspace_id, pipeline_name, pipeline_id, run_id):
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # GET details endpoint
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/Pipeline/instances/{run_id}/details"
    
    print(f"\n==================================================")
    print(f"Details for Pipeline: {pipeline_name} | Run ID: {run_id}")
    print(f"==================================================")
    
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        print(json.dumps(data, indent=2))
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")

def main():
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    # 1. Clinical foundation ingestion
    check_details(
        workspace_id=workspace_id,
        pipeline_name="healthcare1_msft_clinical_data_foundation_ingestion",
        pipeline_id="f8fc260d-6c1f-451b-8763-097fa274b4c0",
        run_id="969a8b9d-4f61-4b5a-a037-44edb929c95f"
    )
    
    # 2. Imaging ingestion
    check_details(
        workspace_id=workspace_id,
        pipeline_name="healthcare1_msft_imaging_with_clinical_foundation_ingestion",
        pipeline_id="d182279d-624f-403a-84ff-ede0e16c486b",
        run_id="3f42da21-a5ba-4874-8c29-eabd4fdecd6a"
    )

if __name__ == "__main__":
    main()
