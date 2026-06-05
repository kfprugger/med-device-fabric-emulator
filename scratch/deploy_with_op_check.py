import requests
import json
import base64
import time
from azure.identity import DefaultAzureCredential

def deploy_and_check():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    
    # Staging parts
    base_dir = "/Users/joey/git/med-device-fabric-emulator/phase-2/omop-research-report/OMOP Academic Research Dashboard.Report"
    parts = []
    
    # 1. Read files and base64-encode
    import os
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file == ".DS_Store":
                continue
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, base_dir).replace("\\", "/")
            
            with open(full_path, "rb") as f:
                content = f.read()
                
            # If definition.pbir, make sure the connection string matches med-0528-f and ExistingModelId
            if rel_path == "definition.pbir":
                # Connection details
                pbir_data = {
                    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
                    "version": "4.0",
                    "datasetReference": {
                        "byConnection": {
                            "connectionString": "Data Source=powerbi://api.powerbi.com/v1.0/myorg/med-0528-f;initial catalog=healthcare1_msft_omop_semantic_model;integrated security=ClaimsToken;semanticmodelid=728137ca-0ed3-4821-824a-a58e58bb69bf"
                        }
                    }
                }
                payload = base64.b64encode(json.dumps(pbir_data).encode("utf-8")).decode("utf-8")
            else:
                payload = base64.b64encode(content).decode("utf-8")
                
            parts.append({
                "path": rel_path,
                "payload": payload,
                "payloadType": "InlineBase64"
            })
            print(f"Staged report part: {rel_path}")

    # Delete existing report named "OMOP Academic Research Dashboard" if any to prevent conflict
    print("Checking for existing report...")
    get_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Report"
    get_resp = requests.get(get_url, headers=headers)
    if get_resp.status_code == 200:
        for item in get_resp.json().get("value", []):
            if item.get("displayName") == "OMOP Academic Research Dashboard":
                print(f"Deleting existing report {item.get('id')}...")
                requests.delete(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item.get('id')}", headers=headers)
                time.sleep(3)

    body = {
        "displayName": "OMOP Academic Research Dashboard",
        "type": "Report",
        "definition": {
            "parts": parts
        }
    }
    
    print("Uploading report definition...")
    resp = requests.post(url, headers=headers, json=body)
    print(f"Response code: {resp.status_code}")
    if resp.status_code == 202:
        location = resp.headers.get("Location")
        op_id = resp.headers.get("x-ms-operation-id")
        poll_url = location if location else f"https://api.fabric.microsoft.com/v1/operations/{op_id}"
        print(f"Initiated async upload. Poll URL: {poll_url}")
        
        start = time.time()
        while time.time() - start < 120:
            time.sleep(5)
            poll_resp = requests.get(poll_url, headers=headers)
            if poll_resp.status_code == 200:
                poll_data = poll_resp.json()
                status = poll_data.get("status", "")
                print(f"Operation status: {status}")
                if status.lower() in ["succeeded", "completed"]:
                    print("✓ Deployment completed successfully!")
                    return
                elif status.lower() in ["failed", "cancelled"]:
                    print(f"✗ Deployment FAILED!")
                    print(json.dumps(poll_data, indent=2))
                    return
            else:
                print(f"Error polling: {poll_resp.status_code} - {poll_resp.text}")
                return
    else:
        print(f"Error {resp.status_code}: {resp.text}")

if __name__ == "__main__":
    deploy_and_check()
