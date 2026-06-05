import requests
import base64
import time
from azure.identity import DefaultAzureCredential

def get_pbir():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    report_id = "60fbba37-6622-415a-83a2-96709ad1a5f5" # ImagingReport
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{report_id}/getDefinition"
    
    resp = requests.post(url, headers=headers)
    if resp.status_code == 202:
        location = resp.headers.get("Location")
        op_id = resp.headers.get("x-ms-operation-id")
        poll_url = location if location else f"https://api.fabric.microsoft.com/v1/operations/{op_id}"
        
        start = time.time()
        while time.time() - start < 60:
            time.sleep(3)
            poll_resp = requests.get(poll_url, headers=headers)
            if poll_resp.status_code == 200:
                poll_data = poll_resp.json()
                if poll_data.get("status", "").lower() in ["succeeded", "completed"]:
                    res_url = f"{poll_url}/result"
                    res_resp = requests.get(res_url, headers=headers)
                    if res_resp.status_code == 200:
                        parts = res_resp.json().get("definition", {}).get("parts", [])
                        for p in parts:
                            if p.get("path") == "definition.pbir":
                                payload = p.get("payload")
                                content = base64.b64decode(payload).decode("utf-8")
                                print("\n--- definition.pbir ---")
                                print(content)
                        return
    else:
        print(f"Error {resp.status_code}: {resp.text}")

if __name__ == "__main__":
    get_pbir()
