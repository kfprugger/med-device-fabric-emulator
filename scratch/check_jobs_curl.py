import subprocess
import json
from azure.identity import DefaultAzureCredential
import sys

def run_curl(url, headers, method="GET", data=None):
    cmd = ["curl", "-s", "-X", method, url]
    for k, v in headers.items():
        cmd.extend(["-H", f"{k}: {v}"])
    if data is not None:
        cmd.extend(["-d", json.dumps(data)])
    elif method == "POST":
        cmd.extend(["-H", "Content-Length: 0"])
    
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"curl error: {res.stderr}")
        return None
    try:
        return json.loads(res.stdout)
    except Exception as e:
        return res.stdout

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    # 1. Get all items in workspace
    items_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    items_res = run_curl(items_url, headers)
    if not items_res or "value" not in items_res:
        print(f"Failed to fetch workspace items: {items_res}")
        sys.exit(1)
        
    notebooks = []
    pipelines = []
    
    for item in items_res.get("value", []):
        if item.get("type") == "Notebook":
            notebooks.append(item)
        elif item.get("type") == "DataPipeline":
            pipelines.append(item)
            
    print(f"Found {len(notebooks)} notebooks and {len(pipelines)} pipelines in workspace.")
    
    # 2. Check recent job instances for notebooks and pipelines
    print("\n--- Recent Runs ---")
    for item in notebooks + pipelines:
        name = item.get("displayName")
        i_id = item.get("id")
        i_type = item.get("type")
        
        # We can try to list instances for RunNotebook job (for Notebooks) or Pipeline job (for Pipelines)
        # Or let's try the general instances endpoint: v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances
        # Notebooks use jobType: RunNotebook
        job_type = "RunNotebook" if i_type == "Notebook" else "Pipeline"
        inst_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{i_id}/jobs/instances?limit=5"
        inst_res = run_curl(inst_url, headers)
        
        if isinstance(inst_res, dict) and "value" in inst_res:
            runs = inst_res["value"]
            if runs:
                print(f"\n{i_type}: {name} ({i_id})")
                for run in runs:
                    r_id = run.get("id")
                    status = run.get("status")
                    start = run.get("startTimeUtc") or run.get("startTime") or "N/A"
                    print(f"  - Run ID: {r_id} | Status: {status} | Start: {start}")
                    if run.get("error"):
                        print(f"    Error: {json.dumps(run.get('error'))}")
            else:
                # print(f"No runs for {name}")
                pass
        else:
            # Try without jobType parameter or print error
            # print(f"Failed/No runs for {name}: {inst_res}")
            pass

if __name__ == "__main__":
    main()
