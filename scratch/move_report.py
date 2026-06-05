import requests
from azure.identity import DefaultAzureCredential

def move_report():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    
    # 1. Find the newly created report
    print("Finding the deployed report ID...")
    get_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Report"
    get_resp = requests.get(get_url, headers=headers)
    report_id = None
    if get_resp.status_code == 200:
        for item in get_resp.json().get("value", []):
            if item.get("displayName") == "OMOP Academic Research Dashboard":
                report_id = item.get("id")
                break
                
    if not report_id:
        print("Could not find the deployed report 'OMOP Academic Research Dashboard' in workspace.")
        return
        
    print(f"Report ID: {report_id}")
    
    # 2. Get or create the folder
    print("Finding or creating folder 'Reports and Semantic Models'...")
    folder_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders"
    folder_resp = requests.get(folder_url, headers=headers)
    folder_id = None
    if folder_resp.status_code == 200:
        for folder in folder_resp.json().get("value", []):
            if folder.get("displayName") == "Reports and Semantic Models":
                folder_id = folder.get("id")
                break
                
    if not folder_id:
        print("Creating folder...")
        create_resp = requests.post(folder_url, headers=headers, json={"displayName": "Reports and Semantic Models"})
        if create_resp.status_code in [200, 201]:
            folder_id = create_resp.json().get("id")
        else:
            print(f"Error creating folder: {create_resp.status_code} - {create_resp.text}")
            return
            
    print(f"Folder ID: {folder_id}")
    
    # 3. Move the report
    print("Moving report into folder...")
    move_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{report_id}/move"
    move_resp = requests.post(move_url, headers=headers, json={"targetFolderId": folder_id})
    if move_resp.status_code in [200, 204]:
        print("✓ Report successfully moved into 'Reports and Semantic Models' folder!")
    else:
        print(f"Error moving report: {move_resp.status_code} - {move_resp.text}")

if __name__ == "__main__":
    move_report()
