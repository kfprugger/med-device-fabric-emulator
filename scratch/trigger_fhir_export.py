import requests
import subprocess
import time
import sys

FHIR_URL = "https://hdwslx36fgkf3gzyw-fhirlx36fgkf3gzyw.fhir.azurehealthcareapis.com"

def get_fhir_token() -> str:
    cmd = ["az", "account", "get-access-token", "--resource", FHIR_URL, "--query", "accessToken", "-o", "tsv"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise Exception(f"Failed to get FHIR access token: {res.stderr}")
    return res.stdout.strip()

def main():
    print("Fetching token...")
    token = get_fhir_token()
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/fhir+json",
        "Prefer": "respond-async"
    }
    
    export_url = f"{FHIR_URL}/$export?_container=fhir-export"
    print(f"Triggering FHIR $export: {export_url} ...")
    
    resp = requests.get(export_url, headers=headers)
    print(f"Response status: {resp.status_code}")
    if resp.status_code == 202:
        content_location = resp.headers.get("Content-Location")
        print(f"✓ Export started successfully. Status URL: {content_location}")
        
        # Poll status
        start_time = time.time()
        print("Polling export progress...")
        while time.time() - start_time < 300: # 5 min timeout
            time.sleep(5)
            poll_resp = requests.get(content_location, headers={"Authorization": f"Bearer {token}"})
            if poll_resp.status_code == 200:
                print("\n✓ Export completed successfully!")
                data = poll_resp.json()
                print("Export outputs:")
                for out in data.get("output", []):
                    print(f"  - File: {out.get('url')} | Type: {out.get('type')}")
                return
            elif poll_resp.status_code == 202:
                # Still running
                sys.stdout.write(".")
                sys.stdout.flush()
            else:
                print(f"\n✗ Export polling failed with status {poll_resp.status_code}: {poll_resp.text}")
                return
        print("\n✗ Export timed out.")
    else:
        print(f"✗ Failed to start export: {resp.status_code} - {resp.text}")

if __name__ == "__main__":
    main()
