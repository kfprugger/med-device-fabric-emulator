#!/usr/bin/env python3
"""
Create Device Associations for existing FHIR data

This script queries the FHIR service for patients with qualifying conditions
(using SNOMED codes that Synthea generates) and creates DeviceAssociation
resources linking them to the already-created Masimo devices.

Run locally with: python create-device-associations.py
"""

import json
import subprocess
import sys
from datetime import datetime

# Configuration
FHIR_SERVICE_URL = "https://hdwsiecaacmlqodcs-fhiriecaacmlqodcs.fhir.azurehealthcareapis.com"
DEVICE_COUNT = 100

# SNOMED codes for conditions that qualify for pulse oximetry monitoring
# These are the actual codes used by Synthea
QUALIFYING_SNOMED_CODES = [
    {"code": "195967001", "display": "Asthma"},
    {"code": "44054006", "display": "Type 2 diabetes mellitus"},
    {"code": "59621000", "display": "Essential hypertension"},
    {"code": "38341003", "display": "Hypertensive disorder"},
    {"code": "162864005", "display": "Body mass index 30+ - obesity"},
    {"code": "271825005", "display": "Respiratory distress"},
    {"code": "840539006", "display": "COVID-19"},
    {"code": "233604007", "display": "Pneumonia"},
    {"code": "13645005", "display": "Chronic obstructive lung disease"},
    {"code": "84114007", "display": "Heart failure"},
    {"code": "22298006", "display": "Myocardial infarction"},
    {"code": "399211009", "display": "History of myocardial infarction"},
    {"code": "53741008", "display": "Coronary arteriosclerosis"},
    {"code": "428007007", "display": "History of heart failure"},
]


def get_access_token():
    """Get Azure access token using az cli"""
    result = subprocess.run(
        ["az", "account", "get-access-token", "--resource", FHIR_SERVICE_URL, "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Error getting access token: {result.stderr}")
        sys.exit(1)
    return result.stdout.strip()


def fhir_request(method, path, token, data=None):
    """Make a request to the FHIR server"""
    import urllib.request
    import urllib.error
    
    url = f"{FHIR_SERVICE_URL}/{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/fhir+json",
        "Accept": "application/fhir+json"
    }
    
    if data:
        data = json.dumps(data).encode('utf-8')
    
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        print(f"HTTP Error {e.code}: {error_body[:500]}")
        raise


def find_qualifying_patients(token, max_patients=100):
    """Find patients with qualifying conditions using SNOMED codes"""
    print("Searching for patients with qualifying conditions...")
    
    qualifying_patients = []
    seen_patient_ids = set()
    
    for snomed in QUALIFYING_SNOMED_CODES:
        if len(qualifying_patients) >= max_patients:
            break
            
        code = snomed['code']
        display = snomed['display']
        
        # Search for conditions with this SNOMED code
        try:
            result = fhir_request(
                "GET",
                f"Condition?code={code}&_include=Condition:subject&_count=50",
                token
            )
            
            conditions_found = 0
            for entry in result.get('entry', []):
                resource = entry.get('resource', {})
                
                if resource.get('resourceType') == 'Patient':
                    patient_id = resource.get('id')
                    if patient_id and patient_id not in seen_patient_ids:
                        seen_patient_ids.add(patient_id)
                        
                        # Get patient name
                        names = resource.get('name', [])
                        name = "Unknown"
                        if names:
                            given = names[0].get('given', [''])[0]
                            family = names[0].get('family', '')
                            name = f"{given} {family}".strip()
                        
                        qualifying_patients.append({
                            'id': patient_id,
                            'name': name,
                            'condition': display
                        })
                        
                        if len(qualifying_patients) >= max_patients:
                            break
                elif resource.get('resourceType') == 'Condition':
                    conditions_found += 1
            
            if conditions_found > 0:
                print(f"  {display} ({code}): {conditions_found} conditions found")
                
        except Exception as e:
            print(f"  Error searching {display}: {e}")
    
    print(f"\nFound {len(qualifying_patients)} qualifying patients")
    return qualifying_patients


def create_device_association(device_id, patient_reference, patient_name):
    """Create a FHIR Basic resource representing DeviceAssociation"""
    return {
        "resourceType": "Basic",
        "id": f"device-assoc-{device_id}",
        "meta": {
            "profile": ["http://hl7.org/fhir/StructureDefinition/Basic"]
        },
        "code": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                "code": "ASSIGNED",
                "display": "assigned device"
            }],
            "text": "Device Assignment"
        },
        "subject": {
            "reference": patient_reference,
            "display": patient_name
        },
        "created": datetime.utcnow().strftime("%Y-%m-%d"),
        "extension": [
            {
                "url": "http://hl7.org/fhir/StructureDefinition/device-association-device",
                "valueReference": {
                    "reference": f"Device/{device_id}",
                    "display": f"Masimo Radius-7 - {device_id}"
                }
            },
            {
                "url": "http://hl7.org/fhir/StructureDefinition/device-association-status",
                "valueCode": "active"
            }
        ]
    }


def create_associations(token, patients):
    """Create DeviceAssociation resources"""
    print(f"\nCreating device associations for {len(patients)} patients...")
    
    created = 0
    failed = 0
    
    for i, patient in enumerate(patients):
        device_id = f"MASIMO-RADIUS7-{(i+1):04d}"
        
        association = create_device_association(
            device_id=device_id,
            patient_reference=f"Patient/{patient['id']}",
            patient_name=patient['name']
        )
        
        try:
            fhir_request("PUT", f"Basic/{association['id']}", token, association)
            created += 1
            
            if (i + 1) % 20 == 0:
                print(f"  Created {i + 1}/{len(patients)} associations...")
                
        except Exception as e:
            print(f"  Failed to create association for {device_id}: {e}")
            failed += 1
    
    print(f"\nCreated {created} device associations ({failed} failed)")
    return created


def verify_associations(token):
    """Verify the created associations"""
    result = fhir_request("GET", "Basic?_summary=count", token)
    count = result.get('total', 0)
    print(f"\nTotal DeviceAssociation (Basic) resources: {count}")
    
    if count > 0:
        # Get a sample
        result = fhir_request("GET", "Basic?_count=2", token)
        print("\nSample associations:")
        for entry in result.get('entry', [])[:2]:
            resource = entry.get('resource', {})
            subject = resource.get('subject', {}).get('display', 'Unknown')
            device_ext = [e for e in resource.get('extension', []) 
                         if 'device-association-device' in e.get('url', '')]
            device = device_ext[0].get('valueReference', {}).get('display', 'Unknown') if device_ext else 'Unknown'
            print(f"  - {device} -> {subject}")


def main():
    print("=" * 60)
    print("DEVICE ASSOCIATION CREATOR")
    print("=" * 60)
    print(f"\nFHIR Service: {FHIR_SERVICE_URL}")
    print(f"Target device associations: {DEVICE_COUNT}")
    
    # Get access token
    print("\nGetting Azure access token...")
    token = get_access_token()
    print("Token acquired successfully")
    
    # Find qualifying patients
    patients = find_qualifying_patients(token, DEVICE_COUNT)
    
    if not patients:
        print("\nERROR: No qualifying patients found!")
        sys.exit(1)
    
    # Create associations
    created = create_associations(token, patients)
    
    # Verify
    verify_associations(token)
    
    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)


if __name__ == '__main__':
    main()
