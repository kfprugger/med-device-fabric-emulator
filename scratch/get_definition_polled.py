import requests
import base64
import time
from azure.identity import DefaultAzureCredential

def get_def():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    model_id = "728137ca-0ed3-4821-824a-a58e58bb69bf"
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{model_id}/getDefinition"
    
    # 1. Post to getDefinition
    print("Requesting definition (async)...")
    resp = requests.post(url, headers=headers)
    if resp.status_code == 202:
        # We need to poll
        location = resp.headers.get("Location")
        op_id = resp.headers.get("x-ms-operation-id")
        poll_url = location if location else f"https://api.fabric.microsoft.com/v1/operations/{op_id}"
        print(f"Operation ID: {op_id}. Polling URL: {poll_url}")
        
        start = time.time()
        while time.time() - start < 120:
            time.sleep(5)
            poll_resp = requests.get(poll_url, headers=headers)
            if poll_resp.status_code == 200:
                poll_data = poll_resp.json()
                status = poll_data.get("status", "").lower()
                print(f"Status: {status}")
                if status in ["succeeded", "completed"]:
                    # Fetch results from the result endpoint
                    result_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{model_id}/getDefinition"
                    # Wait, once succeeded, getDefinition can be fetched or is returned?
                    # Let's try to fetch the definition again or check result details
                    # In Fabric API, the GET /getDefinition or checking the operation result returns it.
                    # Actually, some operations store the result in the operation payload under 'result' or we can fetch getDefinition again.
                    if "result" in poll_data:
                        print("Definition found in operation result!")
                        print_definition_parts(poll_data["result"])
                        return
                    else:
                        # Fetch the final completed definition
                        final_resp = requests.post(url, headers=headers)
                        if final_resp.status_code == 200:
                            print_definition_parts(final_resp.json())
                            return
                        else:
                            print(f"Error fetching final: {final_resp.status_code} - {final_resp.text}")
                            return
                elif status in ["failed", "cancelled"]:
                    print(f"Operation failed: {poll_data}")
                    return
            else:
                print(f"Polling error {poll_resp.status_code}: {poll_resp.text}")
    elif resp.status_code == 200:
        print_definition_parts(resp.json())
    else:
        print(f"Error {resp.status_code}: {resp.text}")

def print_definition_parts(data):
    parts = data.get("definition", {}).get("parts", [])
    print(f"Retrieved {len(parts)} definition parts")
    for part in parts:
        path = part.get("path")
        if path in ["definition/model.tmdl", "definition/expressions.tmdl", "definition/database.tmdl"]:
            payload = part.get("payload")
            content = base64.b64decode(payload).decode("utf-8")
            print(f"\n--- {path} ---")
            print(content)
        elif "person.tmdl" in path:
            payload = part.get("payload")
            content = base64.b64decode(payload).decode("utf-8")
            print(f"\n--- {path} ---")
            print(content[:800])

if __name__ == "__main__":
    get_def()
