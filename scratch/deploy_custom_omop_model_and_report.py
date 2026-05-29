import requests
import base64
import os
import json
import time
from azure.identity import DefaultAzureCredential

def delete_item_if_exists(headers, workspace_id, display_name, item_type):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type={item_type}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        items = resp.json().get("value", [])
        for item in items:
            if item.get("displayName") == display_name:
                item_id = item.get("id")
                print(f"Found existing {item_type} '{display_name}' ({item_id}). Deleting...")
                del_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}"
                del_resp = requests.delete(del_url, headers=headers)
                print(f"Delete response: {del_resp.status_code}")
                time.sleep(3) # Wait for deletion to propagate in Fabric
                return True
    return False

def poll_item_id(headers, workspace_id, display_name, item_type):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type={item_type}"
    start_time = time.time()
    while time.time() - start_time < 60:
        time.sleep(3)
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            items = resp.json().get("value", [])
            for item in items:
                if item.get("displayName") == display_name:
                    return item.get("id")
    raise Exception(f"Timeout waiting for {item_type} '{display_name}' creation in workspace.")

def get_or_create_folder(headers, workspace_id, folder_name):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        folders = resp.json().get("value", [])
        for folder in folders:
            if folder.get("displayName") == folder_name:
                return folder.get("id")
                
    # Create folder if not exists
    print(f"Creating folder '{folder_name}'...")
    create_resp = requests.post(url, headers=headers, json={"displayName": folder_name})
    if create_resp.status_code in [200, 201]:
        return create_resp.json().get("id")
    else:
        raise Exception(f"Failed to create folder: {create_resp.status_code} - {create_resp.text}")

def move_item_to_folder(headers, workspace_id, item_id, folder_id):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/move"
    resp = requests.post(url, headers=headers, json={"targetFolderId": folder_id})
    print(f"Moved item {item_id} into folder response: {resp.status_code}")

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    workspace_name = "med-0528-f"
    
    model_display_name = "OMOP Academic Research Custom Model"
    report_display_name = "OMOP Academic Research Dashboard"
    
    model_dir = "/Users/joey/git/med-device-fabric-emulator/phase-2/omop-research-report/OMOP Academic Research Dashboard.SemanticModel"
    report_dir = "/Users/joey/git/med-device-fabric-emulator/phase-2/omop-research-report/OMOP Academic Research Dashboard.Report"
    
    print("=============================================================")
    print("   DEPLOYING CUSTOM DIRECT LAKE MODEL & DASHBOARD TO FABRIC  ")
    print("=============================================================")
    
    # 1. Clean existing items
    delete_item_if_exists(headers, workspace_id, report_display_name, "Report")
    delete_item_if_exists(headers, workspace_id, model_display_name, "SemanticModel")
    
    # 2. Upload Semantic Model
    print(f"\nGathering semantic model parts from {model_dir} ...")
    model_parts = []
    for root, _, files in os.walk(model_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, model_dir).replace("\\", "/")
            
            if ".DS_Store" in rel_path:
                continue
                
            with open(full_path, "rb") as f:
                content_bytes = f.read()
            base64_payload = base64.b64encode(content_bytes).decode("utf-8")
            
            model_parts.append({
                "path": rel_path,
                "payloadType": "InlineBase64",
                "payload": base64_payload
            })
            print(f"  Staged model part: {rel_path}")
            
    model_body = {
        "displayName": model_display_name,
        "type": "SemanticModel",
        "definition": {
            "parts": model_parts
        }
    }
    
    print("\nUploading custom semantic model to Fabric...")
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    model_resp = requests.post(url, headers=headers, json=model_body)
    print(f"Model upload status: {model_resp.status_code}")
    if model_resp.status_code not in [200, 202]:
        print(f"Error: {model_resp.text}")
        return
        
    print("Polling workspace for model creation...")
    new_model_id = poll_item_id(headers, workspace_id, model_display_name, "SemanticModel")
    print(f"✓ Custom Semantic Model successfully deployed! ID: {new_model_id}")
    
    # 3. Upload Report
    print(f"\nGathering report parts from {report_dir} ...")
    report_parts = []
    for root, _, files in os.walk(report_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, report_dir).replace("\\", "/")
            
            if ".DS_Store" in rel_path:
                continue
                
            if rel_path == "definition.pbir":
                # Patch dataset connection reference to the new custom model ID and display name
                with open(full_path, "r", encoding="utf-8") as f:
                    pbir_obj = json.load(f)
                
                new_conn_str = f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace_name};initial catalog={model_display_name};integrated security=ClaimsToken;semanticmodelid={new_model_id}"
                pbir_obj["datasetReference"]["byConnection"]["connectionString"] = new_conn_str
                
                patched_json_str = json.dumps(pbir_obj, indent=2)
                content_bytes = patched_json_str.encode("utf-8")
                print(f"  Patched connectionString in definition.pbir to: {new_model_id}")
            else:
                with open(full_path, "rb") as f:
                    content_bytes = f.read()
                    
            base64_payload = base64.b64encode(content_bytes).decode("utf-8")
            report_parts.append({
                "path": rel_path,
                "payloadType": "InlineBase64",
                "payload": base64_payload
            })
            print(f"  Staged report part: {rel_path}")
            
    report_body = {
        "displayName": report_display_name,
        "type": "Report",
        "definition": {
            "parts": report_parts
        }
    }
    
    print("\nUploading report to Fabric...")
    report_resp = requests.post(url, headers=headers, json=report_body)
    print(f"Report upload status: {report_resp.status_code}")
    if report_resp.status_code not in [200, 202]:
        print(f"Error: {report_resp.text}")
        return
        
    print("Polling workspace for report creation...")
    new_report_id = poll_item_id(headers, workspace_id, report_display_name, "Report")
    print(f"✓ Report successfully deployed! ID: {new_report_id}")
    
    # 4. Move to folder
    print("\nOrganizing custom items inside 'Reports and Semantic Models' folder...")
    folder_id = get_or_create_folder(headers, workspace_id, "Reports and Semantic Models")
    move_item_to_folder(headers, workspace_id, new_model_id, folder_id)
    move_item_to_folder(headers, workspace_id, new_report_id, folder_id)
    
    print("\n=============================================================")
    print("  OMOP CUSTOM DIRECT LAKE MODEL & DASHBOARD SUCCESSFULLY DEPLOYED!")
    print("=============================================================")

if __name__ == "__main__":
    main()
