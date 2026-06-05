import subprocess
import json
import sys
from azure.identity import DefaultAzureCredential

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
        return None
    try:
        return json.loads(res.stdout)
    except Exception as e:
        return res.stdout

def main():
    if len(sys.argv) < 4:
        print("Usage: python3 get_run_status.py <workspace_id> <item_id> <run_id>")
        sys.exit(1)
        
    workspace_id = sys.argv[1]
    item_id = sys.argv[2]
    run_id = sys.argv[3]
    
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{run_id}"
    res = run_curl(url, headers)
    print(json.dumps(res, indent=2))
    
    # Also fetch details
    url_details = f"{url}/details"
    res_det = run_curl(url_details, headers)
    print("\nDetails:")
    print(json.dumps(res_det, indent=2))

if __name__ == "__main__":
    main()
