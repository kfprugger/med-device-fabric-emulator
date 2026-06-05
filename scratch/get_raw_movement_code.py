import subprocess
import json
import base64
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
    notebook_id = "23dd318b-6608-4d59-a18f-e6359dbaef6e" # raw movement
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/getDefinition"
    
    location = None
    cmd = ["curl", "-s", "-i", "-X", "POST", url, "-H", f"Authorization: Bearer {token}", "-H", "Content-Length: 0"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    for line in res.stdout.splitlines():
        if line.lower().startswith("location:"):
            location = line.split(":", 1)[1].strip()
            break
            
    if not location:
        print(f"Failed to get location: {res.stdout[:500]}")
        return
        
    import time
    start = time.time()
    while time.time() - start < 120:
        time.sleep(3)
        poll_resp = run_curl(location, headers)
        if isinstance(poll_resp, dict) and poll_resp.get("status", "").lower() in ["succeeded", "completed"]:
            res_url = f"{location}/result"
            res_resp = run_curl(res_url, headers)
            if isinstance(res_resp, dict) and "definition" in res_resp:
                parts = res_resp["definition"].get("parts", [])
                for part in parts:
                    path = part.get("path")
                    payload = part.get("payload")
                    if path == "notebook-content.py":
                        decoded = base64.b64decode(payload).decode("utf-8")
                        with open("/Users/joey/git/med-device-fabric-emulator/scratch/raw_movement_code.py", "w", encoding="utf-8") as out:
                            out.write(decoded)
                        print("Wrote raw_movement_code.py")
                        return
            return

if __name__ == "__main__":
    main()
