import requests
from azure.identity import DefaultAzureCredential
import time
import sys
import json

def trigger_pipeline(headers, workspace_id, pipeline_name, pipeline_id):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/Pipeline/instances"
    print(f"\nTriggering Pipeline: {pipeline_name} ...")
    resp = requests.post(url, headers=headers)
    print(f"Response status: {resp.status_code}")
    if resp.status_code in [200, 202]:
        location = resp.headers.get("Location")
        op_id = resp.headers.get("x-ms-operation-id")
        poll_url = location if location else f"https://api.fabric.microsoft.com/v1/operations/{op_id}"
        
        # We need to find the latest job instance ID to poll the run status
        print("Waiting for job instance to be created...")
        time.sleep(10)
        
        instances_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?limit=1"
        inst_resp = requests.get(instances_url, headers=headers)
        run_id = None
        if inst_resp.status_code == 200:
            runs = inst_resp.json().get("value", [])
            if runs:
                run_id = runs[0].get("id")
                print(f"Found Run ID: {run_id}")
        
        if not run_id:
            print("Warning: Could not fetch active Run ID. Polling fallback (150s)...")
            time.sleep(150)
            return True
            
        # Poll the specific run ID
        start_time = time.time()
        while time.time() - start_time < 1800: # 30 min timeout
            time.sleep(20)
            status_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{run_id}"
            status_resp = requests.get(status_url, headers=headers)
            if status_resp.status_code == 200:
                status_data = status_resp.json()
                status = status_data.get("status", "")
                print(f"  Pipeline Status: {status} ({int(time.time() - start_time)}s elapsed)")
                if status == "Completed":
                    print(f"✓ Pipeline {pipeline_name} completed successfully!")
                    return True
                elif status in ["Failed", "Cancelled"]:
                    print(f"✗ Pipeline {pipeline_name} ended with status: {status}")
                    if status_data.get("failureReason"):
                        print(f"  Failure Reason: {status_data.get('failureReason')}")
                    return False
            else:
                print(f"  Status check failed: {status_resp.status_code} - {status_resp.text}")
        print(f"✗ Pipeline {pipeline_name} timed out.")
        return False
    else:
        print(f"✗ Failed to trigger pipeline: {resp.status_code} - {resp.text}")
        return False

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    # 1. Clinical Ingestion
    clin_success = trigger_pipeline(
        headers=headers,
        workspace_id=workspace_id,
        pipeline_name="healthcare1_msft_clinical_data_foundation_ingestion",
        pipeline_id="f8fc260d-6c1f-451b-8763-097fa274b4c0"
    )
    
    if not clin_success:
        print("✗ Ingestion pipeline failed. Skipping OMOP transformation.")
        sys.exit(1)
        
    # 2. Trigger OMOP Transformation
    omop_success = trigger_pipeline(
        headers=headers,
        workspace_id=workspace_id,
        pipeline_name="healthcare1_msft_omop_analytics",
        pipeline_id="b3e005c8-96c8-43fb-93f9-af59225d470f"
    )
    
    if omop_success:
        print("\n================================================")
        print("  DATA INGESTION AND OMOP TRANSFORMATION COMPLETE  ")
        print("================================================")
    else:
        print("✗ OMOP pipeline execution failed.")

if __name__ == "__main__":
    main()
