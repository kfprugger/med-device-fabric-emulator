import requests
from azure.identity import DefaultAzureCredential
import json
import sys

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    notebook_id = "b95a98b9-3351-4f8b-ad1b-21ced09bbcfc" # fhir_ndjson_bronze_ingestion
    run_id = "63aeb334-ff35-4157-9aa4-07cee2ceb84f"
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{run_id}"
    resp = requests.get(url, headers=headers)
    print("--- GET SPARK JOB ---")
    if resp.status_code == 200:
        print(json.dumps(resp.json(), indent=2))
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")
        
    url_details = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{run_id}/details"
    resp_details = requests.get(url_details, headers=headers)
    print("\n--- GET SPARK JOB DETAILS ---")
    if resp_details.status_code == 200:
        print(json.dumps(resp_details.json(), indent=2))
    else:
        print(f"Failed: {resp_details.status_code} - {resp_details.text}")

if __name__ == "__main__":
    main()
