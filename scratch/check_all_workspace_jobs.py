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
    
    # Query all items in the workspace to get displayName lookup
    items_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    items_resp = requests.get(items_url, headers=headers)
    item_names = {}
    if items_resp.status_code == 200:
        for item in items_resp.json().get("value", []):
            item_names[item.get("id")] = (item.get("displayName"), item.get("type"))
            
    # List jobs endpoint: GET /workspaces/{id}/jobs/instances
    # Note: Fabric API might not support listing all jobs at workspace level directly, but let's try it,
    # or list jobs for all notebooks and pipelines.
    print("--- CHECKING WORKSPACE ACTIVE JOBS ---", flush=True)
    
    for item_id, (name, item_type) in item_names.items():
        if item_type in ["Notebook", "DataPipeline"]:
            job_type = "Pipeline" if item_type == "DataPipeline" else "SparkJob" # or try without type
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances?limit=2"
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                instances = resp.json().get("value", [])
                for inst in instances:
                    status = inst.get("status")
                    if status in ["InProgress", "NotStarted", "Queued"]:
                        print(f"ACTIVE: {item_type:12} | Name: {name:50} | Status: {status} | RunID: {inst.get('id')} | Start: {inst.get('startTimeUtc') if 'startTimeUtc' in inst else inst.get('startTime')}", flush=True)
                    elif status in ["Failed"]:
                        print(f"FAILED: {item_type:12} | Name: {name:50} | Status: {status} | RunID: {inst.get('id')} | Error: {json.dumps(inst.get('error'))}", flush=True)
            else:
                # Fallback without job type constraints if 404
                pass

if __name__ == "__main__":
    main()
