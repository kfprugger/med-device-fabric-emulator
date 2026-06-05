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
    
    # List latest instances
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?limit=2"
    resp = requests.get(url, headers=headers)
    print("--- LATEST PIPELINE RUN STATUS ---", flush=True)
    if resp.status_code == 200:
        instances = resp.json().get("value", [])
        if instances:
            for inst in instances:
                print(f"\nRun ID:         {inst.get('id')}", flush=True)
                print(f"Status:         {inst.get('status')}", flush=True)
                print(f"Start Time Utc: {inst.get('startTimeUtc')}", flush=True)
                print(f"End Time Utc:   {inst.get('endTimeUtc')}", flush=True)
                print(f"Failure Reason: {inst.get('failureReason')}", flush=True)
        else:
            print("No instances found.", flush=True)
    else:
        print(f"Failed to list instances: {resp.status_code} - {resp.text}", flush=True)

if __name__ == "__main__":
    main()
