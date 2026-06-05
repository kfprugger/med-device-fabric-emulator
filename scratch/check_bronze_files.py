import requests
from azure.identity import DefaultAzureCredential
import sys

def list_onelake_files(workspace_id, lakehouse_name, lakehouse_id):
    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "x-ms-version": "2020-10-02"
    }
    
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}?recursive=true&resource=filesystem"
    print(f"\n==================================================")
    print(f"Lakehouse: {lakehouse_name} ({lakehouse_id})")
    print(f"==================================================")
    print(f"GET {url}")
    
    resp = requests.get(url, headers=headers)
    print(f"Status Code: {resp.status_code}")
    if resp.status_code == 200:
        paths = resp.json().get("paths", [])
        files = [p for p in paths if int(p.get('contentLength', 0)) > 0]
        print(f"Total files found in Bronze: {len(files)}")
        for path in files[:100]:
            print(f"  - File: {path.get('name'):70} | Length: {path.get('contentLength')}")
        if len(files) > 100:
            print("  ... (truncated output)")
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")

def main():
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    # 1. Bronze
    list_onelake_files(
        workspace_id=workspace_id,
        lakehouse_name="healthcare1_msft_bronze",
        lakehouse_id="245f8768-acdd-4c71-8fbe-6976fe1aa95c"
    )

if __name__ == "__main__":
    main()
