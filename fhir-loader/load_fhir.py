"""
FHIR Loader - Uploads Synthea data to Azure FHIR Service and creates device linkages

This script:
1. Downloads Synthea-generated FHIR bundles from Azure Blob Storage
2. Uploads Atlanta provider organizations  
3. Identifies patients with qualifying conditions for home monitoring
4. Creates FHIR Device resources for Masimo pulse oximeters
5. Creates DeviceAssociation resources linking devices to patients
6. Ensures Children's Healthcare of Atlanta patients are pediatric
"""

import os
import sys
import json
import tempfile
import time
import traceback
from datetime import datetime, date
from typing import List, Dict, Any, Optional

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print("=== FHIR LOADER STARTING ===", flush=True)

import requests
from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Configuration
FHIR_SERVICE_URL = os.getenv('FHIR_SERVICE_URL', '').rstrip('/')
STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT', '')
CONTAINER_NAME = os.getenv('CONTAINER_NAME', 'synthea-output')
DEVICE_COUNT = int(os.getenv('DEVICE_COUNT', '100'))

print(f"FHIR Service URL: {FHIR_SERVICE_URL}", flush=True)
print(f"Storage Account: {STORAGE_ACCOUNT}", flush=True)
print(f"Container Name: {CONTAINER_NAME}", flush=True)
print(f"Device Count: {DEVICE_COUNT}", flush=True)

# Children's Healthcare of Atlanta organization IDs
CHOA_ORG_IDS = [
    'childrens-healthcare-atlanta',
    'choa-egleston', 
    'choa-scottish-rite',
    'choa-hughes-spalding'
]

# Load device registry
with open('/app/device_registry.json', 'r') as f:
    DEVICE_REGISTRY = json.load(f)

# Load Atlanta providers
with open('/app/atlanta_providers.json', 'r') as f:
    ATLANTA_PROVIDERS = json.load(f)


class FHIRClient:
    """Client for interacting with Azure FHIR Service"""
    
    def __init__(self, fhir_url: str):
        self.fhir_url = fhir_url
        self.credential = None
        self.access_token = None
        self.token_expiry = None
        # Use the FHIR URL itself as the resource scope
        self.resource_scope = f"{fhir_url}/.default"
        
    def _get_token(self) -> str:
        """Get access token using Managed Identity"""
        if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
            return self.access_token
            
        try:
            # Try Managed Identity first, fall back to Default
            try:
                self.credential = ManagedIdentityCredential()
                token = self.credential.get_token(self.resource_scope)
            except Exception:
                self.credential = DefaultAzureCredential()
                token = self.credential.get_token(self.resource_scope)
                
            self.access_token = token.token
            # Token typically expires in 1 hour, refresh 5 minutes early
            self.token_expiry = datetime.fromtimestamp(token.expires_on - 300)
            return self.access_token
        except Exception as e:
            print(f"Error getting token: {e}", flush=True)
            raise
    
    def _headers(self) -> Dict[str, str]:
        return {
            'Authorization': f'Bearer {self._get_token()}',
            'Content-Type': 'application/fhir+json',
            'Accept': 'application/fhir+json'
        }
    
    def post_bundle(self, bundle: Dict) -> Dict:
        """Post a transaction bundle to FHIR"""
        response = requests.post(
            self.fhir_url,
            headers=self._headers(),
            json=bundle,
            timeout=300
        )
        if response.status_code not in [200, 201]:
            print(f"Bundle POST failed: {response.status_code} - {response.text[:500]}", flush=True)
        response.raise_for_status()
        return response.json()
    
    def post_resource(self, resource: Dict) -> Dict:
        """Post a single resource to FHIR"""
        resource_type = resource['resourceType']
        response = requests.post(
            f"{self.fhir_url}/{resource_type}",
            headers=self._headers(),
            json=resource,
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    
    def put_resource(self, resource: Dict, resource_id: str) -> Dict:
        """Put (update/create) a resource with specific ID"""
        resource_type = resource['resourceType']
        response = requests.put(
            f"{self.fhir_url}/{resource_type}/{resource_id}",
            headers=self._headers(),
            json=resource,
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    
    def search(self, resource_type: str, params: Dict = None) -> List[Dict]:
        """Search for resources"""
        response = requests.get(
            f"{self.fhir_url}/{resource_type}",
            headers=self._headers(),
            params=params or {},
            timeout=60
        )
        response.raise_for_status()
        result = response.json()
        return result.get('entry', [])
    
    def get_count(self, resource_type: str) -> int:
        """Get count of resources"""
        response = requests.get(
            f"{self.fhir_url}/{resource_type}",
            headers=self._headers(),
            params={'_summary': 'count'},
            timeout=60
        )
        response.raise_for_status()
        return response.json().get('total', 0)


def calculate_age(birth_date_str: str) -> int:
    """Calculate age from birth date string"""
    try:
        birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d').date()
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age
    except:
        return 0


def is_pediatric(patient: Dict) -> bool:
    """Check if patient is under 21 (pediatric)"""
    birth_date = patient.get('birthDate', '')
    if birth_date:
        return calculate_age(birth_date) < 21
    return False


def has_qualifying_condition(bundle: Dict) -> bool:
    """Check if patient bundle has a qualifying condition for home monitoring
    
    Supports both ICD-10 codes (used by some EHRs) and SNOMED CT codes (used by Synthea)
    """
    # Get qualifying codes from registry - support both formats
    qualifying_icd10 = []
    qualifying_snomed = []
    
    qc = DEVICE_REGISTRY.get('qualifyingConditions', {})
    
    # Support old format (codes) and new format (icd10/snomed)
    if 'codes' in qc:
        qualifying_icd10 = [c['code'] for c in qc['codes']]
    if 'icd10' in qc:
        qualifying_icd10 = [c['code'] for c in qc['icd10']]
    if 'snomed' in qc:
        qualifying_snomed = [c['code'] for c in qc['snomed']]
    
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        if resource.get('resourceType') == 'Condition':
            code = resource.get('code', {})
            for coding in code.get('coding', []):
                code_value = coding.get('code', '')
                system = coding.get('system', '')
                
                # Check SNOMED codes (exact match)
                if 'snomed' in system.lower() and code_value in qualifying_snomed:
                    return True
                
                # Check ICD-10 codes (prefix match for hierarchical codes)
                if 'icd' in system.lower():
                    for qc in qualifying_icd10:
                        if code_value.startswith(qc):
                            return True
                
                # Also check if no system specified - try prefix match against ICD-10
                if not system:
                    for qc in qualifying_icd10:
                        if code_value.startswith(qc):
                            return True
    return False


def get_patient_from_bundle(bundle: Dict) -> Optional[Dict]:
    """Extract Patient resource from bundle"""
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        if resource.get('resourceType') == 'Patient':
            return resource
    return None


def get_patient_managing_org(bundle: Dict) -> Optional[str]:
    """Get the managing organization from patient encounters"""
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        if resource.get('resourceType') == 'Encounter':
            service_provider = resource.get('serviceProvider', {})
            ref = service_provider.get('reference', '')
            if ref:
                return ref.split('/')[-1] if '/' in ref else ref
    return None


def is_choa_patient(bundle: Dict) -> bool:
    """Check if patient is associated with Children's Healthcare of Atlanta"""
    org_id = get_patient_managing_org(bundle)
    if org_id:
        for choa_id in CHOA_ORG_IDS:
            if choa_id in org_id.lower():
                return True
    return False


def create_device_resource(device_info: Dict) -> Dict:
    """Create a FHIR Device resource for a Masimo pulse oximeter"""
    return {
        "resourceType": "Device",
        "id": device_info['id'],
        "identifier": [
            {
                "system": "http://masimo.com/devices",
                "value": device_info['id']
            },
            {
                "system": "http://masimo.com/serial-numbers",
                "value": device_info['serialNumber']
            }
        ],
        "status": "active",
        "manufacturer": device_info['manufacturer'],
        "deviceName": [
            {
                "name": f"Masimo {device_info['model']} Pulse Oximeter",
                "type": "user-friendly-name"
            }
        ],
        "modelNumber": device_info['model'],
        "serialNumber": device_info['serialNumber'],
        "type": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "706767009",
                    "display": "Pulse oximeter"
                }
            ],
            "text": "Pulse Oximeter"
        },
        "note": [
            {
                "text": "Measures: SpO2 (oxygen saturation), heart rate, perfusion index"
            }
        ],
        "safety": [
            {
                "coding": [
                    {
                        "system": "urn:oid:2.16.840.1.113883.3.26.1.1",
                        "code": "C113844",
                        "display": "Labeling does not contain latex warning"
                    }
                ]
            }
        ]
    }


def create_device_association(device_id: str, patient_reference: str, patient_name: str) -> Dict:
    """Create a DeviceAssociation linking a device to a patient (FHIR R5 style, R4 compatible)"""
    # Note: In FHIR R4, this would typically be modeled differently
    # Using a custom extension or the Device.patient field
    # For R4 compatibility, we'll use a Basic resource with extensions
    return {
        "resourceType": "Basic",
        "id": f"device-assoc-{device_id}",
        "meta": {
            "profile": ["http://example.org/StructureDefinition/device-association"]
        },
        "code": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/basic-resource-type",
                    "code": "device-assoc",
                    "display": "Device Association"
                }
            ]
        },
        "subject": {
            "reference": patient_reference,
            "display": patient_name
        },
        "extension": [
            {
                "url": "http://example.org/StructureDefinition/associated-device",
                "valueReference": {
                    "reference": f"Device/{device_id}",
                    "display": f"Masimo Radius-7 ({device_id})"
                }
            },
            {
                "url": "http://example.org/StructureDefinition/association-status",
                "valueCode": "active"
            },
            {
                "url": "http://example.org/StructureDefinition/association-period",
                "valuePeriod": {
                    "start": datetime.now().strftime('%Y-%m-%d')
                }
            }
        ]
    }


def upload_providers(client: FHIRClient) -> None:
    """Upload Atlanta provider organizations"""
    print("Uploading Atlanta provider organizations...", flush=True)
    
    for entry in ATLANTA_PROVIDERS.get('entry', []):
        resource = entry.get('resource', {})
        if resource.get('resourceType') == 'Organization':
            try:
                org_id = resource.get('id', '')
                client.put_resource(resource, org_id)
                print(f"  - Uploaded: {resource.get('name', org_id)}", flush=True)
            except Exception as e:
                print(f"  - Failed to upload {resource.get('name', '')}: {e}", flush=True)


def upload_devices(client: FHIRClient) -> None:
    """Upload all device resources"""
    print(f"Uploading {DEVICE_COUNT} Masimo device resources...", flush=True)
    
    devices = DEVICE_REGISTRY['devices'][:DEVICE_COUNT]
    for i, device_info in enumerate(devices):
        try:
            device_resource = create_device_resource(device_info)
            client.put_resource(device_resource, device_info['id'])
            if (i + 1) % 20 == 0:
                print(f"  - Uploaded {i + 1}/{DEVICE_COUNT} devices", flush=True)
        except Exception as e:
            print(f"  - Failed to upload device {device_info['id']}: {e}", flush=True)
    
    print(f"Uploaded {DEVICE_COUNT} devices", flush=True)


def get_blob_service_client():
    """Get blob service client with managed identity"""
    try:
        credential = ManagedIdentityCredential()
        return BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
            credential=credential
        )
    except Exception:
        credential = DefaultAzureCredential()
        return BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
            credential=credential
        )


def stream_synthea_bundles(batch_size: int = 50, max_retries: int = 12, retry_delay: int = 10):
    """Stream Synthea bundles from Azure Blob Storage in batches to avoid OOM.
    Includes retry logic for RBAC propagation on storage."""
    print(f"Streaming Synthea bundles from blob storage...", flush=True)
    print(f"  Storage Account: {STORAGE_ACCOUNT}", flush=True)
    print(f"  Container: {CONTAINER_NAME}", flush=True)
    print(f"  Batch size: {batch_size}", flush=True)
    
    # Retry loop for storage RBAC propagation
    json_blobs = None
    for attempt in range(max_retries):
        try:
            blob_service = get_blob_service_client()
            container_client = blob_service.get_container_client(CONTAINER_NAME)
            
            # List all JSON blobs - this is where RBAC errors occur
            blobs = list(container_client.list_blobs())
            json_blobs = [b for b in blobs if b.name.endswith('.json')]
            print(f"Found {len(json_blobs)} JSON files in blob storage", flush=True)
            break  # Success
        except Exception as e:
            error_str = str(e).lower()
            if 'authorization' in error_str or 'permission' in error_str or '403' in error_str:
                print(f"  Storage attempt {attempt + 1}/{max_retries} failed (RBAC propagating): {e}", flush=True)
                if attempt < max_retries - 1:
                    print(f"  Retrying in {retry_delay} seconds...", flush=True)
                    import time
                    time.sleep(retry_delay)
            else:
                raise
    
    if json_blobs is None:
        raise Exception("Failed to connect to blob storage after retries")
    
    total_blobs = len(json_blobs)
    downloaded_count = 0
    batch = []
    
    for i, blob in enumerate(json_blobs):
        try:
            blob_client = container_client.get_blob_client(blob.name)
            content = blob_client.download_blob().readall()
            bundle = json.loads(content)
            
            if bundle.get('resourceType') == 'Bundle':
                batch.append(bundle)
                downloaded_count += 1
            
            # Yield batch when full
            if len(batch) >= batch_size:
                print(f"  - Downloaded {i + 1}/{total_blobs} files, yielding batch of {len(batch)}", flush=True)
                yield batch
                batch = []
                
        except Exception as e:
            print(f"  - Error downloading {blob.name}: {e}", flush=True)
    
    # Yield remaining bundles
    if batch:
        print(f"  - Yielding final batch of {len(batch)} bundles", flush=True)
        yield batch
    
    print(f"Streamed {downloaded_count} FHIR bundles total", flush=True)


def is_conditional_reference(ref: str) -> bool:
    """Check if a reference is a conditional reference (contains ?)"""
    return isinstance(ref, str) and '?' in ref


def transform_urn_uuid_reference(ref: str) -> str:
    """Transform urn:uuid:XXX references to just XXX.
    E.g., 'Patient/urn:uuid:abc123' becomes 'Patient/abc123'
    and 'urn:uuid:abc123' becomes 'abc123'"""
    if not isinstance(ref, str):
        return ref
    if 'urn:uuid:' in ref:
        return ref.replace('urn:uuid:', '')
    return ref


def extract_conditional_refs_from_resource(obj: Any, refs: set) -> None:
    """Recursively extract all conditional references from a resource."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == 'reference' and isinstance(value, str) and is_conditional_reference(value):
                refs.add(value)
            else:
                extract_conditional_refs_from_resource(value, refs)
    elif isinstance(obj, list):
        for item in obj:
            extract_conditional_refs_from_resource(item, refs)


def create_practitioner_from_npi(npi: str) -> Dict:
    """Create a minimal Practitioner resource from an NPI number."""
    import hashlib
    # Generate a deterministic UUID from the NPI
    uuid = hashlib.md5(f"practitioner-npi-{npi}".encode()).hexdigest()
    uuid_formatted = f"{uuid[:8]}-{uuid[8:12]}-{uuid[12:16]}-{uuid[16:20]}-{uuid[20:32]}"
    
    return {
        "resourceType": "Practitioner",
        "id": uuid_formatted,
        "identifier": [
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": npi
            }
        ],
        "active": True,
        "name": [
            {
                "use": "official",
                "family": f"Provider-{npi[-4:]}",
                "given": ["Healthcare"]
            }
        ]
    }


def create_location_from_ref(location_ref: str) -> Optional[Dict]:
    """Create a minimal Location resource from a conditional reference."""
    import hashlib
    import urllib.parse
    
    # Parse the conditional reference to extract identifier
    # Format: Location?identifier=system|value
    if '?' not in location_ref:
        return None
    
    parts = location_ref.split('?', 1)
    if len(parts) != 2:
        return None
    
    params = urllib.parse.parse_qs(parts[1])
    identifier_value = params.get('identifier', [''])[0]
    
    if not identifier_value or '|' not in identifier_value:
        return None
    
    system, value = identifier_value.split('|', 1)
    
    # Generate deterministic UUID
    uuid = hashlib.md5(f"location-{system}-{value}".encode()).hexdigest()
    uuid_formatted = f"{uuid[:8]}-{uuid[8:12]}-{uuid[12:16]}-{uuid[16:20]}-{uuid[20:32]}"
    
    return {
        "resourceType": "Location",
        "id": uuid_formatted,
        "identifier": [
            {
                "system": system,
                "value": value
            }
        ],
        "status": "active",
        "name": f"Location {value[-6:] if len(value) > 6 else value}"
    }


def create_organization_from_ref(org_ref: str) -> Optional[Dict]:
    """Create a minimal Organization resource from a conditional reference."""
    import hashlib
    import urllib.parse
    
    # Parse the conditional reference to extract identifier
    # Format: Organization?identifier=system|value
    if '?' not in org_ref:
        return None
    
    parts = org_ref.split('?', 1)
    if len(parts) != 2:
        return None
    
    params = urllib.parse.parse_qs(parts[1])
    identifier_value = params.get('identifier', [''])[0]
    
    if not identifier_value or '|' not in identifier_value:
        return None
    
    system, value = identifier_value.split('|', 1)
    
    # Generate deterministic UUID
    uuid = hashlib.md5(f"organization-{system}-{value}".encode()).hexdigest()
    uuid_formatted = f"{uuid[:8]}-{uuid[8:12]}-{uuid[12:16]}-{uuid[16:20]}-{uuid[20:32]}"
    
    return {
        "resourceType": "Organization",
        "id": uuid_formatted,
        "identifier": [
            {
                "system": system,
                "value": value
            }
        ],
        "active": True,
        "name": f"Organization {value[-8:] if len(value) > 8 else value}"
    }


def inject_referenced_resources(bundle: Dict) -> Dict:
    """Scan bundle for conditional references and create missing resources.
    
    This handles the case where Synthea bundles reference Practitioners, Locations,
    and Organizations via conditional references, but don't include those resources 
    in the bundle. We create minimal stub resources so the references can resolve.
    """
    # Collect all conditional references in the bundle
    conditional_refs = set()
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        extract_conditional_refs_from_resource(resource, conditional_refs)
    
    # Group by resource type
    practitioner_npis = set()
    location_refs = set()
    organization_refs = set()
    
    for ref in conditional_refs:
        if ref.startswith('Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|'):
            # Extract NPI
            npi = ref.split('|')[-1]
            practitioner_npis.add(npi)
        elif ref.startswith('Location?'):
            location_refs.add(ref)
        elif ref.startswith('Organization?'):
            organization_refs.add(ref)
    
    # Create new entries for missing resources
    new_entries = []
    
    # Create Organization resources (first, as others may reference them)
    for org_ref in organization_refs:
        org = create_organization_from_ref(org_ref)
        if org:
            new_entries.append({
                'fullUrl': f"urn:uuid:{org['id']}",
                'resource': org
            })
    
    # Create Practitioner resources
    for npi in practitioner_npis:
        practitioner = create_practitioner_from_npi(npi)
        new_entries.append({
            'fullUrl': f"urn:uuid:{practitioner['id']}",
            'resource': practitioner
        })
    
    # Create Location resources  
    for loc_ref in location_refs:
        location = create_location_from_ref(loc_ref)
        if location:
            new_entries.append({
                'fullUrl': f"urn:uuid:{location['id']}",
                'resource': location
            })
    
    # Add new entries at the beginning of the bundle (they need to be processed first)
    if new_entries:
        bundle['entry'] = new_entries + bundle.get('entry', [])
    
    return bundle


def build_conditional_reference_map(bundle: Dict) -> Dict[str, str]:
    """Build a mapping from conditional references to direct UUID references.
    
    Scans all entries in the bundle to find resources with identifiers.
    Creates mappings like:
      'Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|9999801290' -> 'Practitioner/abc-123-uuid'
    """
    ref_map = {}
    
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        resource_type = resource.get('resourceType', '')
        full_url = entry.get('fullUrl', '')
        
        # Get the UUID for this resource
        if full_url.startswith('urn:uuid:'):
            resource_uuid = full_url.replace('urn:uuid:', '')
        elif '/' in full_url:
            resource_uuid = full_url.split('/')[-1]
        else:
            resource_uuid = resource.get('id', '')
        
        if not resource_uuid:
            continue
        
        # For each identifier on this resource, create a conditional reference mapping
        for identifier in resource.get('identifier', []):
            system = identifier.get('system', '')
            value = identifier.get('value', '')
            if system and value:
                # Create the conditional reference format
                conditional_ref = f"{resource_type}?identifier={system}|{value}"
                # Map to direct reference
                direct_ref = f"{resource_type}/{resource_uuid}"
                ref_map[conditional_ref] = direct_ref
    
    return ref_map


def transform_conditional_to_direct(obj: Any, ref_map: Dict[str, str]) -> Any:
    """Recursively transform conditional references to direct references using the map."""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if key == 'reference' and isinstance(value, str) and is_conditional_reference(value):
                # Look up in ref_map
                if value in ref_map:
                    result[key] = ref_map[value]
                else:
                    # Keep the conditional reference if we can't resolve it
                    result[key] = value
            else:
                result[key] = transform_conditional_to_direct(value, ref_map)
        return result
    elif isinstance(obj, list):
        return [transform_conditional_to_direct(item, ref_map) for item in obj]
    else:
        return obj


def transform_references_in_resource(obj: Any) -> Any:
    """Recursively transform all urn:uuid: references in a resource"""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if key == 'reference' and isinstance(value, str):
                result[key] = transform_urn_uuid_reference(value)
            else:
                result[key] = transform_references_in_resource(value)
        return result
    elif isinstance(obj, list):
        return [transform_references_in_resource(item) for item in obj]
    else:
        return obj


def has_any_conditional_reference(resource: Dict) -> bool:
    """Recursively check if a resource or any of its children has conditional references"""
    if isinstance(resource, dict):
        for key, value in resource.items():
            if key == 'reference' and is_conditional_reference(value):
                return True
            if has_any_conditional_reference(value):
                return True
    elif isinstance(resource, list):
        for item in resource:
            if has_any_conditional_reference(item):
                return True
    return False


def reorder_bundle_entries(bundle: Dict) -> Dict:
    """Reorder bundle entries so that referenced resources come first.
    Order: Organization -> Practitioner -> Location -> Patient -> everything else
    This ensures conditional references can resolve properly."""
    
    entries = bundle.get('entry', [])
    
    # Define processing order - referenced resources first
    type_order = {
        'Organization': 0,
        'Practitioner': 1,
        'PractitionerRole': 2,
        'Location': 3,
        'Patient': 4,
    }
    
    def get_order(entry):
        resource_type = entry.get('resource', {}).get('resourceType', '')
        return type_order.get(resource_type, 99)
    
    # Sort entries by type order
    sorted_entries = sorted(entries, key=get_order)
    bundle['entry'] = sorted_entries
    return bundle


def split_bundle_entries(bundle: Dict, max_entries: int = 400) -> List[Dict]:
    """Split a bundle into smaller bundles if it exceeds max entries.
    Uses 400 to stay safely under FHIR's 500 limit."""
    entries = bundle.get('entry', [])
    
    if len(entries) <= max_entries:
        return [bundle]
    
    # Separate entries by type - foundational resources must come first
    # Order: Organization, Practitioner, PractitionerRole, Location, Patient, then others
    type_order = {'Organization': 0, 'Practitioner': 1, 'PractitionerRole': 2, 'Location': 3, 'Patient': 4}
    
    def get_order(e):
        rt = e.get('resource', {}).get('resourceType', '')
        return type_order.get(rt, 99)
    
    # Sort all entries by type order
    sorted_entries = sorted(entries, key=get_order)
    
    # Find where foundational resources end (everything with order < 99)
    foundational = [e for e in sorted_entries if get_order(e) < 99]
    other_entries = [e for e in sorted_entries if get_order(e) >= 99]
    
    bundles = []
    # First bundle gets foundational resources + first batch of other entries
    # Subsequent bundles only get other entries (foundational resources already uploaded)
    for i in range(0, max(1, len(other_entries)), max_entries - len(foundational)):
        chunk = other_entries[i:i + max_entries - len(foundational)]
        new_bundle = {
            'resourceType': 'Bundle',
            'type': bundle.get('type', 'transaction'),
            'entry': foundational + chunk if i == 0 else chunk
        }
        bundles.append(new_bundle)
    
    return bundles


def process_synthea_bundles(client: FHIRClient) -> List[Dict]:
    """Stream and process Synthea bundles, upload to FHIR, and identify qualifying patients"""
    print("Processing Synthea bundles (streaming mode)...", flush=True)
    
    qualifying_patients = []
    uploaded_count = 0
    skipped_choa_adult = 0
    processed_count = 0
    bundle_splits = 0
    
    # Stream bundles in batches to avoid OOM
    for batch in stream_synthea_bundles(batch_size=50):
        for bundle in batch:
            try:
                patient = get_patient_from_bundle(bundle)
                if not patient:
                    continue
                
                # Check CHOA patients - must be pediatric
                if is_choa_patient(bundle) and not is_pediatric(patient):
                    skipped_choa_adult += 1
                    continue  # Skip non-pediatric CHOA patients
                
                # Inject stub Practitioner/Location resources for conditional references
                # Synthea bundles reference these but don't include them
                bundle = inject_referenced_resources(bundle)
                
                # Reorder entries so referenced resources come first
                bundle = reorder_bundle_entries(bundle)
                
                # Build map of conditional references -> direct UUID references
                # This allows us to convert Practitioner?identifier=... to Practitioner/uuid
                ref_map = build_conditional_reference_map(bundle)
                
                # Convert to transaction bundle
                bundle['type'] = 'transaction'
                for entry in bundle.get('entry', []):
                    resource = entry.get('resource', {})
                    resource_type = resource.get('resourceType', '')
                    full_url = entry.get('fullUrl', '')
                    
                    # Extract resource ID - handle urn:uuid: format from Synthea
                    if full_url.startswith('urn:uuid:'):
                        resource_id = full_url.replace('urn:uuid:', '')
                    elif '/' in full_url:
                        resource_id = full_url.split('/')[-1]
                    else:
                        resource_id = resource.get('id', '')
                    
                    # Ensure resource.id matches the URL we'll use
                    if resource_id:
                        resource['id'] = resource_id
                    
                    # Transform all urn:uuid: references within the resource
                    transformed_resource = transform_references_in_resource(resource)
                    
                    # Convert conditional references to direct references
                    transformed_resource = transform_conditional_to_direct(transformed_resource, ref_map)
                    entry['resource'] = transformed_resource
                    
                    entry['request'] = {
                        'method': 'PUT',
                        'url': f"{resource_type}/{resource_id}" if resource_id else resource_type
                    }
                
                # Split large bundles to stay under FHIR's 500 entry limit
                sub_bundles = split_bundle_entries(bundle, max_entries=400)
                if len(sub_bundles) > 1:
                    bundle_splits += 1
                
                # Upload all sub-bundles
                for sub_bundle in sub_bundles:
                    client.post_bundle(sub_bundle)
                uploaded_count += 1
                
                # Check if patient qualifies for device monitoring
                if has_qualifying_condition(bundle) and len(qualifying_patients) < DEVICE_COUNT:
                    patient_name = ''
                    names = patient.get('name', [])
                    if names:
                        name = names[0]
                        given = ' '.join(name.get('given', []))
                        family = name.get('family', '')
                        patient_name = f"{given} {family}".strip()
                    
                    patient_id = patient.get('id', '')
                    qualifying_patients.append({
                        'id': patient_id,
                        'name': patient_name,
                        'birthDate': patient.get('birthDate', ''),
                        'isPediatric': is_pediatric(patient)
                    })
                
                processed_count += 1
                if processed_count % 100 == 0:
                    print(f"  - Processed {processed_count} bundles, uploaded {uploaded_count}, qualifying: {len(qualifying_patients)}, splits: {bundle_splits}", flush=True)
                    
            except Exception as e:
                print(f"  - Error processing bundle: {e}", flush=True)
        
        # Clear batch reference to help GC
        del batch
    
    print(f"Uploaded {uploaded_count} patient bundles", flush=True)
    print(f"Skipped {skipped_choa_adult} non-pediatric CHOA patients", flush=True)
    print(f"Split {bundle_splits} large bundles to stay under FHIR limit", flush=True)
    print(f"Found {len(qualifying_patients)} qualifying patients for device monitoring", flush=True)
    
    return qualifying_patients


def create_device_associations(client: FHIRClient, qualifying_patients: List[Dict]) -> None:
    """Create device associations linking devices to qualifying patients"""
    print(f"Creating device associations for {len(qualifying_patients)} patients...", flush=True)
    
    devices = DEVICE_REGISTRY['devices'][:len(qualifying_patients)]
    
    for i, (device_info, patient) in enumerate(zip(devices, qualifying_patients)):
        try:
            association = create_device_association(
                device_id=device_info['id'],
                patient_reference=f"Patient/{patient['id']}",
                patient_name=patient['name']
            )
            client.put_resource(association, association['id'])
            
            if (i + 1) % 20 == 0:
                print(f"  - Created {i + 1}/{len(qualifying_patients)} associations", flush=True)
                
        except Exception as e:
            print(f"  - Failed to create association for device {device_info['id']}: {e}", flush=True)
    
    print(f"Created {len(qualifying_patients)} device associations", flush=True)


def print_summary(client: FHIRClient) -> None:
    """Print summary of FHIR data"""
    print("\n=== FHIR DATA SUMMARY ===", flush=True)
    
    resource_types = ['Patient', 'Organization', 'Practitioner', 'Encounter', 
                      'Condition', 'Observation', 'Device', 'Basic']
    
    for rt in resource_types:
        try:
            count = client.get_count(rt)
            print(f"  {rt}: {count}", flush=True)
        except Exception as e:
            print(f"  {rt}: Error - {e}", flush=True)


def main():
    print("Initializing FHIR client...", flush=True)
    
    if not FHIR_SERVICE_URL:
        print("ERROR: FHIR_SERVICE_URL not set", flush=True)
        sys.exit(1)
    
    client = FHIRClient(FHIR_SERVICE_URL)
    
    # Test connection with retry for RBAC propagation
    print("Testing FHIR connection (with retry for RBAC propagation)...", flush=True)
    max_retries = 12  # 12 retries * 10 seconds = 2 minutes max wait
    retry_delay = 10
    connected = False
    
    for attempt in range(max_retries):
        try:
            client.get_count('Patient')
            print("FHIR connection successful", flush=True)
            connected = True
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  Attempt {attempt + 1}/{max_retries} failed (RBAC may be propagating): {e}", flush=True)
                print(f"  Retrying in {retry_delay} seconds...", flush=True)
                time.sleep(retry_delay)
            else:
                print(f"ERROR: FHIR connection failed after {max_retries} attempts: {e}", flush=True)
                traceback.print_exc()
                sys.exit(1)
    
    if not connected:
        print("ERROR: Could not establish FHIR connection", flush=True)
        sys.exit(1)
    
    # Step 1: Upload Atlanta providers
    upload_providers(client)
    
    # Step 2: Upload device resources
    upload_devices(client)
    
    # Step 3: Process Synthea bundles and identify qualifying patients
    qualifying_patients = process_synthea_bundles(client)
    
    # Step 4: Create device associations
    if qualifying_patients:
        create_device_associations(client, qualifying_patients)
    else:
        print("WARNING: No qualifying patients found for device associations", flush=True)
    
    # Step 5: Print summary
    print_summary(client)
    
    print("\n=== FHIR LOADER COMPLETE ===", flush=True)


if __name__ == '__main__':
    main()
