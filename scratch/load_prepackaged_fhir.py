import sys
import os
import json
import subprocess
import requests
from datetime import datetime

# Redirect sys.path to load helper routines if needed
sys.path.append("/Users/joey/git/med-device-fabric-emulator/fhir-loader")

# Setup configuration
FHIR_URL = "https://hdwslx36fgkf3gzyw-fhirlx36fgkf3gzyw.fhir.azurehealthcareapis.com"
PREPACKAGED_DIR = "/Users/joey/git/med-device-fabric-emulator/synthea/prepackaged"
FHIR_LOADER_DIR = "/Users/joey/git/med-device-fabric-emulator/fhir-loader"

print(f"Active FHIR Service URL: {FHIR_URL}")
print(f"Prepackaged Patient Directory: {PREPACKAGED_DIR}")

# 1. Fetch OAuth token using Azure CLI
def get_fhir_token() -> str:
    cmd = ["az", "account", "get-access-token", "--resource", FHIR_URL, "--query", "accessToken", "-o", "tsv"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise Exception(f"Failed to get FHIR access token: {res.stderr}")
    return res.stdout.strip()

token = get_fhir_token()
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/fhir+json",
    "Accept": "application/fhir+json"
}

# 2. Load provider organizations
def load_providers():
    print("\n--- Uploading Atlanta provider organizations ---")
    providers_path = os.path.join(FHIR_LOADER_DIR, "atlanta_providers.json")
    with open(providers_path, "r", encoding="utf-8") as f:
        providers = json.load(f)
        
    for entry in providers.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Organization":
            org_id = resource.get("id")
            url = f"{FHIR_URL}/Organization/{org_id}"
            resp = requests.put(url, headers=headers, json=resource)
            if resp.status_code in [200, 201]:
                print(f"  ✓ Uploaded Organization: {resource.get('name')}")
            else:
                print(f"  ✗ Failed for {resource.get('name')}: {resp.status_code} - {resp.text}")

# 3. Load Masimo devices
def load_devices():
    print("\n--- Uploading Masimo device registry resources ---")
    registry_path = os.path.join(FHIR_LOADER_DIR, "device_registry.json")
    with open(registry_path, "r", encoding="utf-8") as f:
        registry = json.load(f)
        
    devices = registry.get("devices", [])[:100]
    for i, dev in enumerate(devices):
        device_resource = {
            "resourceType": "Device",
            "id": dev["id"],
            "identifier": [
                {"system": "http://masimo.com/devices", "value": dev["id"]},
                {"system": "http://masimo.com/serial-numbers", "value": dev["serialNumber"]}
            ],
            "status": "active",
            "manufacturer": dev["manufacturer"],
            "deviceName": [
                {"name": f"Masimo {dev['model']} Pulse Oximeter", "type": "user-friendly-name"}
            ],
            "modelNumber": dev["model"],
            "serialNumber": dev["serialNumber"],
            "type": {
                "coding": [
                    {"system": "http://snomed.info/sct", "code": "706767009", "display": "Pulse oximeter"}
                ],
                "text": "Pulse Oximeter"
            }
        }
        url = f"{FHIR_URL}/Device/{dev['id']}"
        resp = requests.put(url, headers=headers, json=device_resource)
        if resp.status_code not in [200, 201]:
            print(f"  ✗ Failed for Device {dev['id']}: {resp.status_code}")
    print(f"  ✓ Processed {len(devices)} device stubs.")

# 4. Load prepackaged clinical patient bundles
def load_patients():
    print("\n--- Uploading prepackaged patient clinical bundles ---")
    files = [f for f in os.listdir(PREPACKAGED_DIR) if f.endswith(".json")]
    
    import re
    
    # We will upload each bundle as a transaction
    for f_name in files:
        f_path = os.path.join(PREPACKAGED_DIR, f_name)
        with open(f_path, "r", encoding="utf-8") as f:
            bundle = json.load(f)
            
        print(f"  Processing {f_name}...")
        
        # Resolve URN UUID references in bundle to ensure clean linkage
        ref_map = {}
        for entry in bundle.get("entry", []):
            full_url = entry.get("fullUrl", "")
            resource = entry.get("resource", {})
            r_type = resource.get("resourceType")
            r_id = resource.get("id")
            if full_url and r_type and r_id:
                ref_map[full_url] = f"{r_type}/{r_id}"
            
            # Inject request block required for transaction bundles
            if r_type and r_id:
                entry["request"] = {
                    "method": "PUT",
                    "url": f"{r_type}/{r_id}"
                }
                
        # Update references in all resources in bundle
        def update_refs(obj):
            if isinstance(obj, dict):
                for k, v in list(obj.items()):
                    if k == "reference" and isinstance(v, str):
                        if v in ref_map:
                            obj[k] = ref_map[v]
                        elif v.startswith("urn:uuid:"):
                            obj[k] = v.replace("urn:uuid:", "")
                    else:
                        update_refs(v)
            elif isinstance(obj, list):
                for item in obj:
                    update_refs(item)
                    
        update_refs(bundle)
        
        # Resolve conditional references in the bundle prior to posting
        bundle_str = json.dumps(bundle)
        
        # 1. Resolve Practitioners (e.g. Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|9999810009)
        practitioners = re.findall(r'Practitioner\?identifier=http://hl7.org/fhir/sid/us-npi\|(\d+)', bundle_str)
        for npi in set(practitioners):
            practitioner_id = f"practitioner-{npi}"
            pract_res = {
                "resourceType": "Practitioner",
                "id": practitioner_id,
                "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": npi}],
                "name": [{"family": f"Provider-{npi[-4:]}", "given": ["Healthcare"]}]
            }
            url = f"{FHIR_URL}/Practitioner/{practitioner_id}"
            requests.put(url, headers=headers, json=pract_res)
            bundle_str = bundle_str.replace(f"Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|{npi}", f"Practitioner/{practitioner_id}")
            
        # 2. Resolve Locations (e.g. Location?identifier=http://example.org/location-ids|loc-piedmont-atlanta-hospital)
        locations = re.findall(r'Location\?identifier=http://example.org/location-ids\|([a-zA-Z0-9\-]+)', bundle_str)
        for loc_id in set(locations):
            location_uuid = f"location-{loc_id}"
            loc_res = {
                "resourceType": "Location",
                "id": location_uuid,
                "identifier": [{"system": "http://example.org/location-ids", "value": loc_id}],
                "name": f"Location {loc_id.replace('loc-', '').replace('-', ' ').title()}",
                "status": "active"
            }
            url = f"{FHIR_URL}/Location/{location_uuid}"
            requests.put(url, headers=headers, json=loc_res)
            bundle_str = bundle_str.replace(f"Location?identifier=http://example.org/location-ids|{loc_id}", f"Location/{location_uuid}")
            
        bundle = json.loads(bundle_str)
        
        # Post the bundle to FHIR
        resp = requests.post(FHIR_URL, headers=headers, json=bundle)
        if resp.status_code in [200, 201]:
            print(f"    ✓ Successfully loaded bundle: {f_name}")
        else:
            print(f"    ✗ Failed to load bundle {f_name}: {resp.status_code} - {resp.text[:500]}")

if __name__ == "__main__":
    load_providers()
    load_devices()
    load_patients()
    print("\n=== FHIR DATABASES FULLY POPULATED ===")
