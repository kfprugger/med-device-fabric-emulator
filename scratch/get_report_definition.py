import requests
import base64
import time
from azure.identity import DefaultAzureCredential

def get_report_def():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    report_id = "60fbba37-6622-415a-83a2-96709ad1a5f5" # ImagingReport
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{report_id}/getDefinition"
    
    print("Requesting report definition...")
    resp = requests.post(url, headers=headers)
    if resp.status_code == 202:
        location = resp.headers.get("Location")
        op_id = resp.headers.get("x-ms-operation-id")
        poll_url = location if location else f"https://api.fabric.microsoft.com/v1/operations/{op_id}"
        
        # Poll
        start = time.time()
        while time.time() - start < 120:
            time.sleep(5)
            poll_resp = requests.get(poll_url, headers=headers)
            if poll_resp.status_code == 200:
                poll_data = poll_resp.json()
                if poll_data.get("status", "").lower() in ["succeeded", "completed"]:
                    # Fetch results
                    res_url = f"{poll_url}/result"
                    res_resp = requests.get(res_url, headers=headers)
                    if res_resp.status_code == 200:
                        print_def(res_resp.json())
                        return
                    else:
                        print(f"Error fetching result: {res_resp.status_code} - {res_resp.text}")
                        return
    else:
        print(f"Error {resp.status_code}: {resp.text}")

def print_def(data):
    parts = data.get("definition", {}).get("parts", [])
    print(f"Retrieved {len(parts)} report parts")
    for part in parts:
        path = part.get("path")
        if "report.json" in path or "version.json" in path or ".platform" in path:
            payload = part.get("payload")
            content = base64.b64decode(payload).decode("utf-8")
            print(f"\n--- {path} ---")
            print(content)
        elif "page.json" in path:
            payload = part.get("payload")
            content = base64.b64decode(payload).decode("utf-8")
            print(f"\n--- {path} ---")
            print(content[:300])

if __name__ == "__main__":
    get_report_def()
