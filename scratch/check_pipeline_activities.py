import requests
from azure.identity import DefaultAzureCredential
import sys
import json

def get_pipeline_runs(headers, workspace_id, pipeline_id, limit=3):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?limit={limit}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    return []

def print_run_activities(headers, workspace_id, pipeline_name, run_id):
    # Fabric Datapipelines/pipelineruns Endpoint
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/datapipelines/pipelineruns/{run_id}/queryactivityruns"
    
    print(f"\n=================================================================================")
    print(f"Activities for {pipeline_name} | Run ID: {run_id}")
    print(f"=================================================================================")
    
    resp = requests.post(url, headers=headers, json={})
    if resp.status_code == 200:
        activities = resp.json().get("value", [])
        print(f"Total activity runs: {len(activities)}")
        for act in sorted(activities, key=lambda x: x.get("activityRunStart", "")):
            status = act.get('status')
            status_char = "✓" if status == "Succeeded" else "✗" if status in ["Failed", "Cancelled"] else "⋯"
            print(f"\n{status_char} Activity: {act.get('activityName'):40}")
            print(f"  Type:     {act.get('activityType')}")
            print(f"  Status:   {status}")
            print(f"  Start:    {act.get('activityRunStart')}")
            print(f"  End:      {act.get('activityRunEnd')}")
            if act.get("error"):
                print(f"  Error:    {json.dumps(act.get('error'), indent=2)}")
    else:
        print(f"Failed to query activity runs: {resp.status_code} - {resp.text}")

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    pipelines = {
        "Clinical Ingestion": "f8fc260d-6c1f-451b-8763-097fa274b4c0",
        "OMOP Analytics": "b3e005c8-96c8-43fb-93f9-af59225d470f"
    }
    
    for name, pipeline_id in pipelines.items():
        runs = get_pipeline_runs(headers, workspace_id, pipeline_id, limit=1)
        if runs:
            latest_run = runs[0]
            print(f"\n>>> Latest {name} Run Status: {latest_run.get('status')} (Started: {latest_run.get('startTimeUtc') if 'startTimeUtc' in latest_run else latest_run.get('startTime')})")
            print_run_activities(headers, workspace_id, name, latest_run.get('id'))
        else:
            print(f"\n>>> No runs found for {name}")

if __name__ == "__main__":
    main()
