import requests
from azure.identity import DefaultAzureCredential

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        lakehouses = resp.json().get("value", [])
        print("Lakehouses in med-0528-f:")
        for lh in lakehouses:
            lh_id = lh.get("id")
            lh_name = lh.get("displayName")
            detail_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lh_id}"
            det_resp = requests.get(detail_url, headers=headers)
            if det_resp.status_code == 200:
                props = det_resp.json().get("properties", {})
                sql_props = props.get("sqlEndpointProperties", {})
                conn_str = sql_props.get("connectionString")
                print(f"Name: {lh_name:45} | ID: {lh_id} | Server: {conn_str}")
            else:
                print(f"Name: {lh_name:45} | Failed to fetch details: {det_resp.status_code}")
    else:
        print(f"Error listing lakehouses: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
