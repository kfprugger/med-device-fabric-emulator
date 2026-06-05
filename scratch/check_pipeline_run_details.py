import requests
from azure.identity import DefaultAzureCredential
import sys
import json

def check_run_details(workspace_id, pipeline_name, pipeline_id, run_id):
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Fabric Datapipelines/pipelineruns Endpoint
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/datapipelines/pipelineruns/{run_id}/queryactivityruns"
    
    print(f"\n==================================================")
    print(f"Details for Pipeline: {pipeline_name} | Run ID: {run_id}")
    print(f"==================================================")
    
    resp = requests.post(url, headers=headers, json={})
    if resp.status_code == 200:
        activities = resp.json().get("value", [])
        print(f"Total activity runs: {len(activities)}")
        for act in sorted(activities, key=lambda x: x.get("activityRunStart", "")):
            print(f"\n* Activity: {act.get('activityName'):40}")
            print(f"  Type:     {act.get('activityType')}")
            print(f"  Status:   {act.get('status')}")
            print(f"  Start:    {act.get('activityRunStart')}")
            print(f"  End:      {act.get('activityRunEnd')}")
            if act.get("error"):
                print(f"  Error:    {json.dumps(act.get('error'), indent=2)}")
    else:
        print(f"Failed to query activity runs: {resp.status_code} - {resp.text}")

def main():
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    # 1. Clinical foundation ingestion
    check_run_details(
        workspace_id=workspace_id,
        pipeline_name="healthcare1_msft_clinical_data_foundation_ingestion",
        pipeline_id="f8fc260d-6c1f-451b-8763-097fa274b4c0",
        run_id="62655b75-aa23-4b3b-8d48-ba5f6efa2a1d"
    )
    
    # 2. Imaging ingestion
    check_run_details(
        workspace_id=workspace_id,
        pipeline_name="healthcare1_msft_imaging_with_clinical_foundation_ingestion",
        pipeline_id="d182279d-624f-403a-84ff-ede0e16c486b",
        run_id="3f42da21-a5ba-4874-8c29-eabd4fdecd6a"
    )

if __name__ == "__main__":
    main()
