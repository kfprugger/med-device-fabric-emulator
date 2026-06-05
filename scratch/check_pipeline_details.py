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
    pipeline_id = "f8fc260d-6c1f-451b-8763-097fa274b4c0" # Clinical ingestion
    
    # First, list the latest job instances
    url_instances = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?limit=1"
    r_inst = requests.get(url_instances, headers=headers)
    if r_inst.status_code == 200:
        instances = r_inst.json().get("value", [])
        if instances:
            latest = instances[0]
            run_id = latest.get("id")
            print(f"Latest Ingestion Run ID: {run_id} | Status: {latest.get('status')}")
            
            # Fetch instance detail
            url_detail = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/Pipeline/instances/{run_id}"
            r_det = requests.get(url_detail, headers=headers)
            print(f"\n--- GET Instance ---")
            if r_det.status_code == 200:
                print(json.dumps(r_det.json(), indent=2))
            else:
                print(f"GET Instance failed: {r_det.status_code} - {r_det.text}")
                
            # Fetch details
            url_details = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/Pipeline/instances/{run_id}/details"
            r_details = requests.get(url_details, headers=headers)
            print(f"\n--- GET Instance Details ---")
            if r_details.status_code == 200:
                print(json.dumps(r_details.json(), indent=2))
            else:
                print(f"GET Instance Details failed: {r_details.status_code} - {r_details.text}")
        else:
            print("No instances found.")
    else:
        print(f"Failed to get instances: {r_inst.status_code} - {r_inst.text}")

if __name__ == "__main__":
    main()
