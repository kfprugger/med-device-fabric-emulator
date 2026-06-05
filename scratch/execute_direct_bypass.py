import subprocess
import json
from azure.identity import DefaultAzureCredential
import sys
import time
import os
import shutil

def run_cmd(cmd):
    print(f"Running command: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Error: {res.stderr}")
        return False
    print("✓ Success")
    return True

def run_curl(url, headers, method="GET", data=None):
    cmd = ["curl", "-s", "-X", method, url]
    for k, v in headers.items():
        cmd.extend(["-H", f"{k}: {v}"])
    if data is not None:
        cmd.extend(["-d", json.dumps(data)])
    elif method == "POST":
        cmd.extend(["-H", "Content-Length: 0"])
        
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"curl command failed: {res.stderr}")
        return None
    try:
        return json.loads(res.stdout)
    except Exception:
        return res.stdout

def get_headers_fabric():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def trigger_and_poll_notebook(workspace_id, notebook_name, notebook_id, parameters=None):
    print(f"\n=================================================================================")
    print(f"Processing Notebook: {notebook_name} ({notebook_id}) ...")
    print(f"=================================================================================")
    
    headers = get_headers_fabric()
    
    # 1. Check if there is already an active run (NotStarted, InProgress, Queued)
    url_instances = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?limit=5"
    resp_inst = run_curl(url_instances, headers)
    
    run_id = None
    if isinstance(resp_inst, dict) and "value" in resp_inst:
        for inst in resp_inst.get("value", []):
            status = inst.get("status")
            if status in ["InProgress", "NotStarted", "Queued"]:
                run_id = inst.get("id")
                print(f"✓ Found and adopting existing active run: {run_id} (Status: {status})")
                break
                
    if not run_id:
        # Trigger a new run
        url_trigger = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/RunNotebook/instances?jobType=RunNotebook"
        print("No active run found. Triggering a new run...")
        
        payload = {}
        if parameters:
            payload = {"executionData": {"parameters": parameters}}
            
        resp = run_curl(url_trigger, headers, method="POST", data=payload if parameters else None)
        print(f"Trigger response: {resp}")
        
        print("Waiting 15 seconds for job instance to register in history...")
        time.sleep(15)
        
        # Get the latest run ID
        headers = get_headers_fabric()
        resp_inst = run_curl(url_instances, headers)
        if isinstance(resp_inst, dict) and "value" in resp_inst:
            instances = resp_inst.get("value", [])
            if instances:
                run_id = instances[0].get("id")
                print(f"Found triggered Run ID: {run_id} | Status: {instances[0].get('status')}")
                
    if not run_id:
        print("Warning: Could not fetch run ID. Polling fallback (120s)...")
        time.sleep(120)
        return True, False, "No run ID found"
        
    # 2. Poll the job instance run status
    start_time = time.time()
    # 45 minutes timeout to allow slow Fabric cold-starts
    while time.time() - start_time < 2700:
        time.sleep(30)
        
        # Dynamically refresh OAuth tokens on every poll step to prevent 401 token expired
        try:
            headers = get_headers_fabric()
        except Exception as ex:
            print(f"Warning: Failed to refresh token: {ex}. Retrying next cycle...")
            continue
            
        url_status = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{run_id}"
        status_data = run_curl(url_status, headers)
        if isinstance(status_data, dict) and "status" in status_data:
            status = status_data.get("status", "")
            print(f"  Notebook {notebook_name} status: {status} ({int(time.time() - start_time)}s elapsed)")
            if status == "Completed":
                print(f"✓ Notebook {notebook_name} completed successfully!")
                return True, False, None
            elif status in ["Failed", "Cancelled"]:
                print(f"✗ Notebook {notebook_name} ended with status: {status}")
                fail_reason = status_data.get("failureReason")
                if fail_reason:
                    print(f"  Failure Reason: {fail_reason}")
                    msg = str(fail_reason).upper()
                    # Check if failure is transient
                    if "CLUSTER_CREATION" in msg or "CLUSTER" in msg or "CAPACITY" in msg or "RESOURCE" in msg or "TIMEOUT" in msg:
                        return False, True, fail_reason
                return False, False, fail_reason
        else:
            print(f"  Status check failed or returned empty: {status_data}")
            
    print(f"✗ Notebook {notebook_name} timed out.")
    return False, True, "Timeout reached"

def trigger_and_poll_notebook_with_retries(workspace_id, notebook_name, notebook_id, parameters=None, max_retries=3):
    for attempt in range(1, max_retries + 1):
        print(f"\n=================================================================================")
        print(f"ATTEMPT {attempt} OF {max_retries} FOR NOTEBOOK: {notebook_name}")
        print(f"=================================================================================")
        
        success, is_transient, fail_reason = trigger_and_poll_notebook(workspace_id, notebook_name, notebook_id, parameters)
        if success:
            return True
            
        if not is_transient:
            print(f"✗ Non-transient failure detected. Aborting pipeline execution.")
            return False
            
        print(f"⚠ Transient/retriable failure detected: {fail_reason}")
        if attempt < max_retries:
            print(f"Waiting 60 seconds before retrying execution on next attempt...")
            time.sleep(60)
            
    print(f"✗ Notebook {notebook_name} failed after {max_retries} attempts.")
    return False

def main():
    # Clear local dirs first
    shutil.rmtree("./scratch/downloaded_ndjson", ignore_errors=True)
    shutil.rmtree("./scratch/zipped_ndjson", ignore_errors=True)
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    # Trigger and poll HDS Spark Notebooks in sequence!
    
    # Activity 0: healthcare1_msft_raw_process_movement
    # notebook ID: 23dd318b-6608-4d59-a18f-e6359dbaef6e
    mov_success = trigger_and_poll_notebook_with_retries(
        workspace_id=workspace_id,
        notebook_name="healthcare1_msft_raw_process_movement",
        notebook_id="23dd318b-6608-4d59-a18f-e6359dbaef6e"
    )
    if not mov_success:
        print("✗ Raw process movement failed.")
        sys.exit(1)
        
    # Activity 1: healthcare1_msft_fhir_ndjson_bronze_ingestion
    # notebook ID: b95a98b9-3351-4f8b-ad1b-21ced09bbcfc
    ing_success = trigger_and_poll_notebook_with_retries(
        workspace_id=workspace_id,
        notebook_name="healthcare1_msft_fhir_ndjson_bronze_ingestion",
        notebook_id="b95a98b9-3351-4f8b-ad1b-21ced09bbcfc"
    )
    if not ing_success:
        print("✗ Ingestion failed.")
        sys.exit(1)
        
    # Activity 2: healthcare1_msft_bronze_silver_flatten
    # notebook ID: 5f4e30bb-b632-42b3-bdd7-27a87453a3fe
    flat_success = trigger_and_poll_notebook_with_retries(
        workspace_id=workspace_id,
        notebook_name="healthcare1_msft_bronze_silver_flatten",
        notebook_id="5f4e30bb-b632-42b3-bdd7-27a87453a3fe"
    )
    if not flat_success:
        print("✗ Flattening failed.")
        sys.exit(1)
        
    # Activity 3: healthcare1_msft_omop_silver_gold_transformation
    # notebook ID: b5e8084b-e1ae-4f21-9251-2385d07da24b
    omop_success = trigger_and_poll_notebook_with_retries(
        workspace_id=workspace_id,
        notebook_name="healthcare1_msft_omop_silver_gold_transformation",
        notebook_id="b5e8084b-e1ae-4f21-9251-2385d07da24b"
    )
    if not omop_success:
        print("✗ OMOP transformation failed.")
        sys.exit(1)
        
    # Activity 4: materialize_reporting_tables
    # notebook ID: 3b4f93b2-d293-4ee8-b35f-31273c04abd6
    mat_success = trigger_and_poll_notebook_with_retries(
        workspace_id=workspace_id,
        notebook_name="materialize_reporting_tables",
        notebook_id="3b4f93b2-d293-4ee8-b35f-31273c04abd6"
    )
    if not mat_success:
        print("✗ Materializing reporting tables failed.")
        sys.exit(1)
        
    # Activity 5: create_device_association_table
    # notebook ID: 3172b080-692c-4ae6-80f9-aab5ba6582ac
    dev_success = trigger_and_poll_notebook_with_retries(
        workspace_id=workspace_id,
        notebook_name="create_device_association_table",
        notebook_id="3172b080-692c-4ae6-80f9-aab5ba6582ac"
    )
    
    if dev_success:
        print("\n================================================")
        print("  DATA INGESTION AND OMOP HYDRATION BYPASS COMPLETE  ")
        print("================================================")
    else:
        print("✗ Creating device association table failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
