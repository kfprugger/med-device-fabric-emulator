import requests
from azure.identity import DefaultAzureCredential
import json

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "x-ms-version": "2020-10-02"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    lakehouse_id = "da39a3e9-aa08-4935-9d5e-a82b2ce2c38a" # Silver lh
    
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/Tables?recursive=true&resource=filesystem"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        paths = resp.json().get("paths", [])
        tables = set()
        med_req_logs = []
        proc_logs = []
        for p in paths:
            name = p.get("name", "")
            parts = name.split("/")
            if len(parts) >= 3:
                tables.add(parts[2])
            if "MedicationRequest/_delta_log" in name:
                med_req_logs.append(name)
            if "Procedure/_delta_log" in name:
                proc_logs.append(name)
        print("Delta tables physically present in Silver Lakehouse OneLake:")
        for t in sorted(list(tables)):
            print(f"  - {t}")
        print("\nMedicationRequest delta log files:")
        for ml in sorted(med_req_logs):
            print(f"  {ml}")
        print("\nProcedure delta log files:")
        for pl in sorted(proc_logs):
            print(f"  {pl}")
    else:
        print(f"Failed to list: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
