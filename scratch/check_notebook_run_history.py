import requests
from azure.identity import DefaultAzureCredential
import sys
import json

def check_notebook_runs(workspace_id, notebook_name, notebook_id):
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?limit=5"
    print(f"\n==================================================")
    print(f"Notebook: {notebook_name} ({notebook_id})")
    print(f"==================================================")
    
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        runs = resp.json().get("value", [])
        print(f"Total runs found: {len(runs)}")
        for run in runs:
            run_id = run.get("id")
            print(f"  - Run ID: {run_id} | Status: {run.get('status')} | Start: {run.get('startTimeUtc') if 'startTimeUtc' in run else run.get('startTime')}")
            if run.get("error"):
                print(f"    Error: {json.dumps(run.get('error'))}")
                
            # Query details for this notebook job run
            det_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{run_id}/details"
            det_resp = requests.get(det_url, headers=headers)
            if det_resp.status_code == 200:
                print("    Job details:")
                print(f"      {json.dumps(det_resp.json(), indent=2)[:1000]}")
    else:
        print(f"Failed: {resp.status_code} - {resp.text}")

def main():
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    notebooks = {
        "healthcare1_msft_raw_process_movement": "23dd318b-6608-4d59-a18f-e6359dbaef6e",
        "healthcare1_msft_fhir_ndjson_bronze_ingestion": "b95a98b9-3351-4f8b-ad1b-21ced09bbcfc",
        "healthcare1_msft_bronze_silver_flatten": "5f4e30bb-b632-42b3-bdd7-27a87453a3fe"
    }
    
    for name, n_id in notebooks.items():
        check_notebook_runs(workspace_id, name, n_id)

if __name__ == "__main__":
    main()
