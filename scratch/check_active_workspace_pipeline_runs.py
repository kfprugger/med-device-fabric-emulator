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
    
    # 1. Get all items in the workspace to find pipelines
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"Failed to list items: {resp.status_code} - {resp.text}")
        sys.exit(1)
        
    items = resp.json().get("value", [])
    pipelines = [i for i in items if i.get("type") == "DataPipeline"]
    
    print(f"Found {len(pipelines)} pipelines in active workspace:")
    for pipe in pipelines:
        pipe_id = pipe.get("id")
        pipe_name = pipe.get("displayName")
        print(f"\n==================================================")
        print(f"Pipeline: {pipe_name} ({pipe_id})")
        print(f"==================================================")
        
        runs_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipe_id}/jobs/instances?limit=5"
        runs_resp = requests.get(runs_url, headers=headers)
        if runs_resp.status_code == 200:
            runs = runs_resp.json().get("value", [])
            print(f"Total runs found: {len(runs)}")
            for run in runs:
                run_id = run.get("id")
                print(f"  - Run ID: {run_id}")
                print(f"    Status: {run.get('status')}")
                print(f"    Start:  {run.get('startTimeUtc') if 'startTimeUtc' in run else run.get('startTime')}")
                print(f"    End:    {run.get('endTimeUtc') if 'endTimeUtc' in run else run.get('endTime')}")
                if run.get("error"):
                    print(f"    Error:  {json.dumps(run.get('error'))}")
        else:
            print(f"Failed to get runs: {runs_resp.status_code} - {runs_resp.text}")

if __name__ == "__main__":
    main()
