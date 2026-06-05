import requests
import base64
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
    
    # POST to getDefinition (it uses POST)
    resp = requests.post(url, headers=headers)
    if resp.status_code == 200:
        parts = resp.json().get("definition", {}).get("parts", [])
        print(f"Retrieved {len(parts)} definition parts")
        for part in parts:
            path = part.get("path")
            if path in ["definition/model.tmdl", "definition/expressions.tmdl", "definition/database.tmdl"]:
                payload = part.get("payload")
                content = base64.b64decode(payload).decode("utf-8")
                print(f"\n--- {path} ---")
                print(content)
            elif "person.tmdl" in path or "Measures" in path:
                payload = part.get("payload")
                content = base64.b64decode(payload).decode("utf-8")
                print(f"\n--- {path} ---")
                print(content[:600])
    else:
        print(f"Error {resp.status_code}: {resp.text}")

if __name__ == "__main__":
    get_def()
