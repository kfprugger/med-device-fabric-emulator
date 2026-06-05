import requests
from azure.identity import DefaultAzureCredential
import base64
import json

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    pipeline_id = "f8fc260d-6c1f-451b-8763-097fa274b4c0" # Clinical Ingestion
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/getDefinition"
    print(f"POST {url}")
    resp = requests.post(url, headers=headers)
    print(f"Status Code: {resp.status_code}")
    if resp.status_code == 202:
        # Async operation polling
        location = resp.headers.get("Location")
        op_id = resp.headers.get("x-ms-operation-id")
        poll_url = location if location else f"https://api.fabric.microsoft.com/v1/operations/{op_id}"
        print(f"Async operation URL: {poll_url}")
        
        import time
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
                        for part in parts:
                            path = part.get("path")
                            payload = part.get("payload")
                            print(f"\nPart Path: {path}")
                            if path == "pipeline-content.json":
                                decoded_str = base64.b64decode(payload).decode("utf-8")
                                try:
                                    pipeline_json = json.loads(decoded_str)
                                    print(json.dumps(pipeline_json, indent=2))
                                except Exception as e:
                                    print(decoded_str[:2000])
                        return
                    else:
                        print(f"Failed to fetch result: {res_resp.status_code} - {res_resp.text}")
                        return
    else:
        print(f"Error: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
