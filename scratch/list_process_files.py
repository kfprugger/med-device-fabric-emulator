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
        print("--- PROCESS CLINICAL FILES AUDIT ---")
        
        target = "Files/Process/Clinical/FHIR-NDJSON/FHIR-HDS/2026/05/29/20260529T184608-14"
        matches = [p for p in paths if target in p.get("name", "") and p.get("isDirectory", "false") != "true"]
        
        print(f"Total files found in processed directory: {len(matches)}")
        for path in sorted(matches, key=lambda x: x.get('name')):
            print(f"  - File: {path.get('name'):110} | Length: {path.get('contentLength')}")
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
