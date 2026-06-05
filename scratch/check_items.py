import requests
from azure.identity import DefaultAzureCredential

def check_items():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        items = resp.json().get("value", [])
        print(f"Total items in workspace: {len(items)}")
        for item in sorted(items, key=lambda x: (x.get("type", ""), x.get("displayName", ""))):
            print(f" - {item.get('type').padRight(20) if hasattr('', 'padRight') else item.get('type'):<18} : {item.get('displayName'):<40} ({item.get('id')})")
    else:
        print(f"Error {resp.status_code}: {resp.text}")

if __name__ == "__main__":
    check_items()
