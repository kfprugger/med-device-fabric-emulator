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
    notebook_id = "4aad4153-0827-4219-a8f7-e4091625e22d" # config notebook
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/getDefinition"
    print(f"POST {url}")
    resp = requests.post(url, headers=headers)
    print(f"Status: {resp.status_code}")
    if resp.status_code == 202:
        location = resp.headers.get("Location")
        op_id = resp.headers.get("x-ms-operation-id")
        poll_url = location if location else f"https://api.fabric.microsoft.com/v1/operations/{op_id}"
        
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
                            print(f"\nPart: {path}")
                            if path == "notebook-content.py":
                                decoded = base64.b64decode(payload).decode("utf-8")
                                print(decoded[:2000])
                                # Save to file
                                with open("/Users/joey/git/med-device-fabric-emulator/scratch/hds_config_raw.py", "w", encoding="utf-8") as out:
                                    out.write(decoded)
                                print("Wrote hds_config_raw.py")
                        return
                    else:
                        print(f"Failed: {res_resp.status_code}")
                        return
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
