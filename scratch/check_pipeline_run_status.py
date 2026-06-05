import requests
from azure.identity import DefaultAzureCredential
import sys
import json

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    pipeline_id = "f8fc260d-6c1f-451b-8763-097fa274b4c0"
    run_id = "cff9e622-5ae0-46ba-a9b7-6a07102f88af"
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{run_id}"
    resp = requests.get(url, headers=headers)
    
    print("--- PIPELINE STATUS QUERY ---", flush=True)
    if resp.status_code == 200:
        data = resp.json()
        print(f"Pipeline Run ID: {run_id}", flush=True)
        print(f"Status:          {data.get('status')}", flush=True)
        print(f"Start Time:      {data.get('startTimeUtc')}", flush=True)
        print(f"End Time:        {data.get('endTimeUtc')}", flush=True)
        print(f"Failure Reason:  {data.get('failureReason')}", flush=True)
    else:
        print(f"Failed: {resp.status_code} - {resp.text}", flush=True)

if __name__ == "__main__":
    main()
