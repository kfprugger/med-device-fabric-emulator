import requests
from azure.identity import DefaultAzureCredential
import json

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "x-ms-version": "2020-10-02"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    lakehouse_id = "245f8768-acdd-4c71-8fbe-6976fe1aa95c"
    
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}?recursive=true&resource=filesystem"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        paths = resp.json().get("paths", [])
        print("--- ONELAKE FILES SEARCH ---")
        
        ingest_paths = [p for p in paths if "Ingest" in p.get("name", "")]
        process_paths = [p for p in paths if "Process" in p.get("name", "")]
        
        print(f"Total Ingest paths found: {len(ingest_paths)}")
        for path in sorted(ingest_paths, key=lambda x: x.get('name'))[:50]:
            print(f"  - Ingest: {path.get('name')}")
            
        print(f"\nTotal Process paths found: {len(process_paths)}")
        for path in sorted(process_paths, key=lambda x: x.get('name'))[:50]:
            print(f"  - Process: {path.get('name')}")
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
