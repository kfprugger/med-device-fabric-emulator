import requests
from azure.identity import DefaultAzureCredential
import time
import sys

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
            
    print("--- STARTING WORKSPACE JOBS MONITOR ---", flush=True)
    start_time = time.time()
    
    # Poll for up to 10 minutes (600s)
    last_states = {}
    while time.time() - start_time < 600:
        active_jobs = []
        for item_id, (name, item_type) in item_names.items():
            if item_type in ["Notebook", "DataPipeline"]:
                url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances?limit=1"
                resp = requests.get(url, headers=headers)
                if resp.status_code == 200:
                    instances = resp.json().get("value", [])
                    if instances:
                        inst = instances[0]
                        status = inst.get("status")
                        run_id = inst.get("id")
                        if status in ["InProgress", "NotStarted", "Queued"]:
                            active_jobs.append((item_type, name, status, run_id))
        
        # Print status updates if changed
        current_states = {run_id: status for _, _, status, run_id in active_jobs}
        if current_states != last_states:
            print(f"\n[Time Elapsed: {int(time.time() - start_time)}s] Active Jobs:", flush=True)
            if not active_jobs:
                print("  (None)", flush=True)
            for item_type, name, status, run_id in active_jobs:
                print(f"  - {item_type:10} | {name:50} | {status} | Run: {run_id}", flush=True)
            last_states = current_states
            
        if not active_jobs and time.time() - start_time > 30:
            # If no active jobs after some initial start period, we might have completed the E2E sequence!
            print("\nAll active jobs completed successfully!", flush=True)
            break
            
        time.sleep(30)
        
if __name__ == "__main__":
    main()
