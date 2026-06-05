import requests
from azure.identity import DefaultAzureCredential
import json

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    env_id = "894d3ef8-5976-4b7b-9ff5-8fac8e262664"
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{env_id}"
    resp = requests.get(url, headers=headers)
    print("--- GET ENVIRONMENT ---")
    if resp.status_code == 200:
        print(json.dumps(resp.json(), indent=2))
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")
        
    # Also check environment libraries or status if supported
    url_libs = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{env_id}/libraries"
    resp_libs = requests.get(url_libs, headers=headers)
    print("\n--- GET ENVIRONMENT LIBRARIES ---")
    if resp_libs.status_code == 200:
        print(json.dumps(resp_libs.json(), indent=2))
    else:
        print(f"Failed: {resp_libs.status_code} - {resp_libs.text}")

if __name__ == "__main__":
    main()
