import requests
from azure.identity import DefaultAzureCredential

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
        print("--- INGEST CLINICAL SHORTCUT FILES ---")
        
        target = "Files/Ingest/Clinical/FHIR-NDJSON/FHIR-HDS"
        matches = [p for p in paths if target in p.get("name", "") and p.get("isDirectory", "false") != "true"]
        
        print(f"Total files found in shortcut: {len(matches)}")
        for path in sorted(matches, key=lambda x: x.get('name')):
            print(f"  - File: {path.get('name'):100} | Length: {path.get('contentLength')}")
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
