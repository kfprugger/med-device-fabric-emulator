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
    
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/Tables/ClinicalFhir?recursive=true&resource=filesystem"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        paths = resp.json().get("paths", [])
        subdirs = set()
        for p in paths:
            name = p.get("name", "")
            if "resourceType=" in name:
                parts = name.split("/")
                for part in parts:
                    if "resourceType=" in part:
                        subdirs.add(part)
        print("Partitions found in Tables/ClinicalFhir:")
        for s in sorted(list(subdirs)):
            print(f"  - {s}")
    else:
        print(f"Failed to list: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
