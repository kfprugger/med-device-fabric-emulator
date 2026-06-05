import subprocess
import json
from azure.identity import DefaultAzureCredential

def run_curl(url, headers, method="GET", data=None):
    cmd = ["curl", "-s", "-X", method, url]
    for k, v in headers.items():
        cmd.extend(["-H", f"{k}: {v}"])
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        return None
    try:
        return json.loads(res.stdout)
    except Exception:
        return res.stdout

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
    resp = run_curl(url, headers)
    if isinstance(resp, dict) and "value" in resp:
        lakehouses = resp.get("value", [])
        print("Lakehouses in workspace:")
        for lh in lakehouses:
            lh_id = lh.get("id")
            lh_name = lh.get("displayName")
            detail_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lh_id}"
            det_resp = run_curl(detail_url, headers)
            if isinstance(det_resp, dict) and "properties" in det_resp:
                props = det_resp.get("properties", {})
                sql_props = props.get("sqlEndpointProperties", {})
                conn_str = sql_props.get("connectionString")
                print(f"Name: {lh_name:45} | ID: {lh_id} | Server: {conn_str}")
    else:
        print(f"Error: {resp}")

if __name__ == "__main__":
    main()
