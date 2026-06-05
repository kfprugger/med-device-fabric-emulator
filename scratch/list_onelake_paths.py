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
    print(f"GET {url}")
    resp = requests.get(url, headers=headers)
    print(f"Status Code: {resp.status_code}")
    if resp.status_code == 200:
        paths = resp.json().get("paths", [])
        print(f"Total paths returned: {len(paths)}")
        for path in sorted(paths, key=lambda x: x.get('name')):
            is_dir = path.get('isDirectory', 'false') == 'true'
            type_str = "DIR " if is_dir else "FILE"
            print(f"  - [{type_str}] {path.get('name'):80} | Length: {path.get('contentLength')}")
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
