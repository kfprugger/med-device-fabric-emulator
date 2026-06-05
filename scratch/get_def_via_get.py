import requests
from azure.identity import DefaultAzureCredential

def get_def_via_get():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    model_id = "728137ca-0ed3-4821-824a-a58e58bb69bf"
    
    # Try GET instead of POST
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{model_id}/getDefinition"
    
    resp = requests.get(url, headers=headers)
    print(f"Status Code: {resp.status_code}")
    print(resp.text[:5000])

if __name__ == "__main__":
    get_def_via_get()
